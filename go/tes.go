package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/net/http2"
)

// Book 书籍信息结构
type Book struct {
	BookNumber   string
	ChapterCount int
}

// RateLimiter 主机级限速器
type RateLimiter struct {
	limiter *rate.Limiter
	host    string
}

// CrawlerNode 爬虫节点
type CrawlerNode struct {
	NodeID string
	DB     *sql.DB
	Client *http.Client

	// 主机级限速 (防止 403/429)
	hostLimiter *rate.Limiter

	// 固定Worker池
	workerPool chan struct{}
	jobQueue   chan CrawlJob

	// 优雅关闭标志
	shutdown int32 // 原子操作标志
}

// CrawlJob 爬取任务
type CrawlJob struct {
	BookNumber string
	ChapterNum int
	URL        string
	ResultChan chan ChapterResult
}

// ChapterResult 章节结果结构
type ChapterResult struct {
	BookNumber string // 添加书号字段
	ChapterNum int
	Title      string
	Content    string
	Error      error
}

// CompiledRegex 预编译的正则表达式
var (
	titleRegex    = regexp.MustCompile(`<title>([^_]+)`)
	contentRegex1 = regexp.MustCompile(`(?s)<div[^>]*id=["']chaptercontent["'][^>]*>(.*?)</div>`)
	contentRegex2 = regexp.MustCompile(`(?s)<div[^>]*class=["'][^"']*content[^"']*["'][^>]*>(.*?)</div>`)
	contentRegex3 = regexp.MustCompile(`(?s)<div[^>]*>([^<]{100,}.*?)</div>`)
	htmlTagRegex  = regexp.MustCompile(`<[^>]*>`)
)

// NewCrawlerNode 创建爬虫节点
func NewCrawlerNode(nodeID string) (*CrawlerNode, error) {
	// dsn := "root:123456@tcp(localhost:3306)/novel_db?charset=utf8mb4&parseTime=True&loc=Local&maxAllowedPacket=0"
	dsn := "user_1:Aa123456@tcp(rm-cn-gh64dx1hy0003l5o.rwlb.rds.aliyuncs.com:3306)/novel_db?charset=utf8mb4&parseTime=True&loc=Local&maxAllowedPacket=0"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("数据库连接失败: %v", err)
	}

	// 配置数据库连接池 - 高并发优化
	db.SetMaxOpenConns(200)                 // 最大连接数 4倍提升
	db.SetMaxIdleConns(100)                 // 最大空闲连接数 4倍提升
	db.SetConnMaxLifetime(10 * time.Minute) // 连接最大生存时间

	// 创建直连HTTP客户端 (HTTP/2 + Keep-Alive)
	transport := &http.Transport{
		// 连接池优化
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 100,              // 增大同主机连接数
		MaxConnsPerHost:     200,              // 限制每主机最大连接
		IdleConnTimeout:     90 * time.Second, // 延长空闲超时

		// 连接建立优化
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second, // TCP Keep-Alive
		}).DialContext,

		// HTTP优化
		DisableKeepAlives:     false,
		DisableCompression:    true, // 禁用压缩减少CPU
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,

		// TLS优化
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // 跳过证书验证加速
		},

		// 强制 HTTP/2
		ForceAttemptHTTP2: true,
	}

	// 配置 HTTP/2
	http2.ConfigureTransport(transport)

	client := &http.Client{
		Timeout:   15 * time.Second,
		Transport: transport,
	}

	// 创建主机级限速器 - 更保守的设置以提高稳定性
	hostLimiter := rate.NewLimiter(rate.Limit(100), 150) // 降低请求频率：每秒100请求，突发150

	// 固定Worker池配置 - 保守设置以提高稳定性
	const numWorkers = 100    // 降低到100个worker，避免资源争抢
	const jobQueueSize = 1000 // 适当减少任务队列大小

	workerPool := make(chan struct{}, numWorkers)
	jobQueue := make(chan CrawlJob, jobQueueSize)

	node := &CrawlerNode{
		NodeID:      nodeID,
		DB:          db,
		Client:      client,
		hostLimiter: hostLimiter,
		workerPool:  workerPool,
		jobQueue:    jobQueue,
	}

	// 启动固定数量的worker协程
	for i := 0; i < numWorkers; i++ {
		go node.crawlWorker()
	}

	return node, nil
}

// GetNextTask 使用SELECT FOR UPDATE获取下一个任务
func (node *CrawlerNode) GetNextTask() (*Book, error) {
	tx, err := node.DB.Begin()
	if err != nil {
		return nil, fmt.Errorf("开始事务失败: %v", err)
	}
	defer tx.Rollback()

	// 使用SELECT FOR UPDATE锁定行
	query := `SELECT book_number, chapter_count 
			  FROM books 
			  WHERE crawl_status = 0 
			  ORDER BY CAST(book_number AS UNSIGNED) ASC 
			  LIMIT 1 FOR UPDATE`

	var book Book
	err = tx.QueryRow(query).Scan(&book.BookNumber, &book.ChapterCount)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // 没有任务了
		}
		return nil, fmt.Errorf("查询任务失败: %v", err)
	}

	// 更新状态为爬取中
	updateQuery := `UPDATE books SET crawl_status = 1 WHERE book_number = ?`
	_, err = tx.Exec(updateQuery, book.BookNumber)
	if err != nil {
		return nil, fmt.Errorf("更新任务状态失败: %v", err)
	}

	// 提交事务
	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("提交事务失败: %v", err)
	}

	return &book, nil
}

// GenerateChapterURL 生成单个章节URL
func GenerateChapterURL(bookNumber string, chapterNum int) string {
	return fmt.Sprintf("https://www.a9db770f8.lol/book/%s/%d.html", bookNumber, chapterNum)
}

// CrawlChapter 爬取单个章节（带重试机制）
func (node *CrawlerNode) CrawlChapter(bookNumber string, chapterNum int) (string, string, error) {
	maxRetries := 3
	baseDelay := 1 * time.Second

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		url := GenerateChapterURL(bookNumber, chapterNum)

		// 创建请求并设置头信息
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return "", "", fmt.Errorf("创建请求失败: %v", err)
		}

		// 设置请求头，模拟真实浏览器
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
		req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
		req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
		req.Header.Set("Accept-Encoding", "gzip, deflate")
		req.Header.Set("Connection", "keep-alive")
		req.Header.Set("Upgrade-Insecure-Requests", "1")

		resp, err := node.Client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("请求失败: %v", err)
			if attempt < maxRetries {
				delay := time.Duration(attempt+1) * baseDelay
				fmt.Printf("[%s] 章节 %s-%d 请求失败，%v后重试 (第%d次)\n",
					node.NodeID, bookNumber, chapterNum, delay, attempt+1)
				time.Sleep(delay)
				continue
			}
			return "", "", lastErr
		}
		defer resp.Body.Close()

		// 处理不同的HTTP状态码
		switch resp.StatusCode {
		case 200:
			// 成功，继续处理
			break
		case 404:
			// 章节不存在，不重试
			return "", "", fmt.Errorf("章节不存在 (404)")
		case 403:
			// 被封禁，等待更长时间后重试
			if attempt < maxRetries {
				delay := time.Duration(5*(attempt+1)) * time.Second
				fmt.Printf("[%s] 章节 %s-%d 访问被拒绝(403)，%v后重试 (第%d次)\n",
					node.NodeID, bookNumber, chapterNum, delay, attempt+1)
				time.Sleep(delay)
				continue
			}
			return "", "", fmt.Errorf("访问被拒绝 (403)")
		case 429:
			// 请求过多，等待后重试
			if attempt < maxRetries {
				delay := time.Duration(10*(attempt+1)) * time.Second
				fmt.Printf("[%s] 章节 %s-%d 请求过多(429)，%v后重试 (第%d次)\n",
					node.NodeID, bookNumber, chapterNum, delay, attempt+1)
				time.Sleep(delay)
				continue
			}
			return "", "", fmt.Errorf("请求过多 (429)")
		case 500, 502, 503, 504:
			// 服务器错误，重试
			if attempt < maxRetries {
				delay := time.Duration(2*(attempt+1)) * time.Second
				fmt.Printf("[%s] 章节 %s-%d 服务器错误(%d)，%v后重试 (第%d次)\n",
					node.NodeID, bookNumber, chapterNum, resp.StatusCode, delay, attempt+1)
				time.Sleep(delay)
				continue
			}
			return "", "", fmt.Errorf("服务器错误 (%d)", resp.StatusCode)
		default:
			// 其他错误
			if attempt < maxRetries {
				delay := time.Duration(attempt+1) * baseDelay
				fmt.Printf("[%s] 章节 %s-%d HTTP错误(%d)，%v后重试 (第%d次)\n",
					node.NodeID, bookNumber, chapterNum, resp.StatusCode, delay, attempt+1)
				time.Sleep(delay)
				continue
			}
			return "", "", fmt.Errorf("HTTP状态码: %d", resp.StatusCode)
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", "", fmt.Errorf("读取响应失败: %v", err)
		}

		html := string(body)

		// 提取章节标题
		titleRe := regexp.MustCompile(`<title>([^_]+)`)
		titleMatches := titleRe.FindStringSubmatch(html)
		title := fmt.Sprintf("第%d章", chapterNum)
		if len(titleMatches) > 1 {
			title = strings.TrimSpace(titleMatches[1])
		}

		// 提取章节内容 - 尝试多种选择器
		var content string

		// 尝试1: id="chaptercontent"
		contentRe1 := regexp.MustCompile(`(?s)<div[^>]*id=["']chaptercontent["'][^>]*>(.*?)</div>`)
		matches1 := contentRe1.FindStringSubmatch(html)
		if len(matches1) > 1 {
			content = matches1[1]
		}

		if content == "" {
			// 调试：保存HTML到文件查看结构
			fmt.Printf("调试：保存HTML到 debug_%s_%d.html\n", bookNumber, chapterNum)
			ioutil.WriteFile(fmt.Sprintf("debug_%s_%d.html", bookNumber, chapterNum), body, 0644)
			return title, "", fmt.Errorf("未找到章节内容")
		}

		// 清理HTML标签
		content = regexp.MustCompile(`<[^>]*>`).ReplaceAllString(content, "")
		content = strings.ReplaceAll(content, "&nbsp;", " ")
		content = strings.ReplaceAll(content, "&#160;", " ")
		content = strings.ReplaceAll(content, "\r", "")
		content = strings.ReplaceAll(content, "\n\n", "\n")
		content = strings.TrimSpace(content)

		return title, content, nil
	}

	return "", "", fmt.Errorf("重试失败")
}

// crawlWorker 固定worker协程
func (node *CrawlerNode) crawlWorker() {
	for job := range node.jobQueue {
		// 主机级限速
		node.hostLimiter.Wait(context.Background())

		title, content, err := node.crawlChapterOptimized(job.URL, job.BookNumber, job.ChapterNum)

		job.ResultChan <- ChapterResult{
			BookNumber: job.BookNumber,
			ChapterNum: job.ChapterNum,
			Title:      title,
			Content:    content,
			Error:      err,
		}
	}
}

// crawlChapterOptimized 优化的章节爬取（带重试机制）
func (node *CrawlerNode) crawlChapterOptimized(url string, bookNumber string, chapterNum int) (string, string, error) {
	maxRetries := 5 // 增加重试次数
	baseDelay := 1 * time.Second

	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		// 检查是否需要关闭
		if node.IsShutdown() {
			return "", "", fmt.Errorf("爬虫正在关闭")
		}

		// 根据尝试次数调整超时时间
		timeout := time.Duration(5+attempt*2) * time.Second // 5s, 7s, 9s, 11s, 13s, 15s
		ctx, cancel := context.WithTimeout(context.Background(), timeout)

		// 创建请求并设置头信息
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			cancel()
			return "", "", fmt.Errorf("创建请求失败: %v", err)
		}

		// 设置请求头，模拟真实浏览器
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
		req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
		req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
		req.Header.Set("Connection", "keep-alive")
		req.Header.Set("Cache-Control", "no-cache")

		// 发送请求
		resp, err := node.Client.Do(req)
		cancel() // 立即取消context

		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				lastErr = fmt.Errorf("请求超时(%v)", timeout)
			} else {
				lastErr = fmt.Errorf("请求失败: %v", err)
			}

			if attempt < maxRetries {
				delay := baseDelay * time.Duration(1<<uint(attempt)) // 指数退避：1s, 2s, 4s, 8s, 16s
				fmt.Printf("[%s] ⚠️  章节 %s-%d %v，%v后重试 (第%d次)\n",
					node.NodeID, bookNumber, chapterNum, lastErr, delay, attempt+1)
				time.Sleep(delay)
				continue
			}
			return "", "", lastErr
		}

		// 检查HTTP状态码并分类处理
		switch resp.StatusCode {
		case 200:
			// 成功，继续处理
			break
		case 404:
			// 章节不存在，不重试
			resp.Body.Close()
			return "", "", fmt.Errorf("章节不存在 (404)")
		case 403:
			// 被封禁，使用更长的等待时间
			resp.Body.Close()
			if attempt < maxRetries {
				delay := time.Duration(10*(attempt+1)) * time.Second // 10s, 20s, 30s, 40s, 50s
				fmt.Printf("[%s] 🚫 章节 %s-%d 访问被拒绝(403)，%v后重试 (第%d次)\n",
					node.NodeID, bookNumber, chapterNum, delay, attempt+1)
				time.Sleep(delay)
				continue
			}
			return "", "", fmt.Errorf("访问被拒绝 (403)")
		case 429:
			// 请求过多，使用更长的等待时间
			resp.Body.Close()
			if attempt < maxRetries {
				delay := time.Duration(15*(attempt+1)) * time.Second // 15s, 30s, 45s, 60s, 75s
				fmt.Printf("[%s] 🚨 章节 %s-%d 请求过多(429)，%v后重试 (第%d次)\n",
					node.NodeID, bookNumber, chapterNum, delay, attempt+1)
				time.Sleep(delay)
				continue
			}
			return "", "", fmt.Errorf("请求过多 (429)")
		case 500, 502, 503, 504:
			// 服务器错误，中等等待时间
			resp.Body.Close()
			if attempt < maxRetries {
				delay := time.Duration(5*(attempt+1)) * time.Second // 5s, 10s, 15s, 20s, 25s
				fmt.Printf("[%s] 🔴 章节 %s-%d 服务器错误(%d)，%v后重试 (第%d次)\n",
					node.NodeID, bookNumber, chapterNum, resp.StatusCode, delay, attempt+1)
				time.Sleep(delay)
				continue
			}
			return "", "", fmt.Errorf("服务器错误 (%d)", resp.StatusCode)
		default:
			// 其他HTTP错误
			resp.Body.Close()
			if attempt < maxRetries {
				delay := time.Duration(2*(attempt+1)) * time.Second
				fmt.Printf("[%s] ❓ 章节 %s-%d HTTP错误(%d)，%v后重试 (第%d次)\n",
					node.NodeID, bookNumber, chapterNum, resp.StatusCode, delay, attempt+1)
				time.Sleep(delay)
				continue
			}
			return "", "", fmt.Errorf("HTTP状态码: %d", resp.StatusCode)
		}

		// 限制响应体大小，防止读取超大文件
		limitedBody := http.MaxBytesReader(nil, resp.Body, 2*1024*1024) // 限制2MB
		body, err := ioutil.ReadAll(limitedBody)
		resp.Body.Close()

		if err != nil {
			lastErr = fmt.Errorf("读取响应失败: %v", err)
			if attempt < maxRetries {
				delay := baseDelay * time.Duration(1+attempt)
				fmt.Printf("[%s] 📖 章节 %s-%d 读取失败，%v后重试 (第%d次)\n",
					node.NodeID, bookNumber, chapterNum, delay, attempt+1)
				time.Sleep(delay)
				continue
			}
			return "", "", lastErr
		}

		html := string(body)

		// 验证内容长度，避免获取到空页面
		if len(html) < 100 {
			lastErr = fmt.Errorf("页面内容过短 (%d 字符)", len(html))
			if attempt < maxRetries {
				delay := baseDelay * time.Duration(1+attempt)
				fmt.Printf("[%s] 📄 章节 %s-%d 内容过短，%v后重试 (第%d次)\n",
					node.NodeID, bookNumber, chapterNum, delay, attempt+1)
				time.Sleep(delay)
				continue
			}
			return "", "", lastErr
		}

		// 使用预编译正则快速提取标题
		title := fmt.Sprintf("第%d章", chapterNum)
		if titleMatch := titleRegex.FindStringSubmatch(html); len(titleMatch) > 1 {
			title = strings.TrimSpace(titleMatch[1])
		}

		// 使用预编译正则快速提取内容
		var content string
		if match := contentRegex1.FindStringSubmatch(html); len(match) > 1 {
			content = match[1]
		} else if match := contentRegex2.FindStringSubmatch(html); len(match) > 1 {
			content = match[1]
		} else if matches := contentRegex3.FindAllStringSubmatch(html, -1); len(matches) > 0 {
			// 选择最长的内容
			for _, match := range matches {
				if len(match[1]) > len(content) {
					content = match[1]
				}
			}
		}

		if content == "" {
			lastErr = fmt.Errorf("未找到章节内容")
			if attempt < maxRetries {
				delay := baseDelay * time.Duration(1+attempt)
				fmt.Printf("[%s] 🔍 章节 %s-%d 内容解析失败，%v后重试 (第%d次)\n",
					node.NodeID, bookNumber, chapterNum, delay, attempt+1)
				time.Sleep(delay)
				continue
			}
			return title, "", lastErr
		}

		// 使用预编译正则快速清理HTML
		content = htmlTagRegex.ReplaceAllString(content, "")
		content = strings.ReplaceAll(content, "&nbsp;", " ")
		content = strings.ReplaceAll(content, "&#160;", " ")
		content = strings.ReplaceAll(content, "\r", "")
		content = strings.ReplaceAll(content, "\n\n", "\n")
		content = strings.TrimSpace(content)

		// 最终验证内容长度
		if len(content) < 10 {
			lastErr = fmt.Errorf("章节内容过短 (%d 字符)", len(content))
			if attempt < maxRetries {
				delay := baseDelay * time.Duration(1+attempt)
				fmt.Printf("[%s] 📝 章节 %s-%d 最终内容过短，%v后重试 (第%d次)\n",
					node.NodeID, bookNumber, chapterNum, delay, attempt+1)
				time.Sleep(delay)
				continue
			}
			return title, "", lastErr
		}

		// 成功获取章节内容
		return title, content, nil
	}

	// 所有重试都失败
	fmt.Printf("[%s] ❌ 章节 %s-%d 重试 %d 次后最终失败: %v\n",
		node.NodeID, bookNumber, chapterNum, maxRetries, lastErr)
	return "", "", fmt.Errorf("重试 %d 次后失败: %v", maxRetries, lastErr)
}

// BatchSaveChapters 批量保存章节到数据库
func (node *CrawlerNode) BatchSaveChapters(bookID string, chapters []ChapterResult) error {
	if len(chapters) == 0 {
		return nil
	}

	// 构建批量插入SQL
	query := `INSERT INTO chapters (book_id, chapter_name, chapter_order, content) VALUES `
	values := []interface{}{}

	for i, chapter := range chapters {
		if i > 0 {
			query += ","
		}
		query += "(?, ?, ?, ?)"
		values = append(values, bookID, chapter.Title, chapter.ChapterNum, chapter.Content)
	}

	query += ` ON DUPLICATE KEY UPDATE content = VALUES(content)`

	_, err := node.DB.Exec(query, values...)
	return err
}

// LogFailedChapter 记录失败章节用于补爬（增加错误信息和重试次数）
func (node *CrawlerNode) LogFailedChapter(bookNumber string, chapterNum int, url string, errorMsg string) error {
	// 先检查是否已存在该失败记录
	checkQuery := `SELECT retry_count FROM failed_chapters WHERE book_number = ? AND chapter_number = ?`
	var retryCount int
	err := node.DB.QueryRow(checkQuery, bookNumber, chapterNum).Scan(&retryCount)

	if err == sql.ErrNoRows {
		// 不存在，插入新记录
		query := `INSERT INTO failed_chapters (book_number, chapter_number, url, error_message, retry_count, failed_at, last_retry_at) 
				  VALUES (?, ?, ?, ?, 1, NOW(), NOW())`
		_, err = node.DB.Exec(query, bookNumber, chapterNum, url, errorMsg)
		return err
	} else if err == nil {
		// 已存在，更新重试次数和错误信息
		updateQuery := `UPDATE failed_chapters 
						SET retry_count = retry_count + 1, 
							error_message = ?, 
							last_retry_at = NOW() 
						WHERE book_number = ? AND chapter_number = ?`
		_, err = node.DB.Exec(updateQuery, errorMsg, bookNumber, chapterNum)
		return err
	}

	return err
}

// SaveChapter 保存章节到数据库
func (node *CrawlerNode) SaveChapter(bookID string, chapterTitle string, content string, chapterOrder int) error {
	query := `INSERT INTO chapters (book_id, chapter_name, chapter_order, content) 
			  VALUES (?, ?, ?, ?) 
			  ON DUPLICATE KEY UPDATE content = VALUES(content)`

	_, err := node.DB.Exec(query, bookID, chapterTitle, chapterOrder, content)
	return err
}

// AsyncDBSaver 异步数据库保存器
type AsyncDBSaver struct {
	saveChan chan []ChapterResult
	node     *CrawlerNode
	wg       sync.WaitGroup
}

// NewAsyncDBSaver 创建异步数据库保存器
func NewAsyncDBSaver(node *CrawlerNode) *AsyncDBSaver {
	saver := &AsyncDBSaver{
		saveChan: make(chan []ChapterResult, 10), // 缓冲10个批次
		node:     node,
	}

	// 启动后台保存协程
	saver.wg.Add(1)
	go saver.saveWorker()

	return saver
}

// saveWorker 后台保存工作协程
func (s *AsyncDBSaver) saveWorker() {
	defer s.wg.Done()

	for chapters := range s.saveChan {
		if len(chapters) == 0 {
			continue
		}

		bookNumber := chapters[0].BookNumber // 从章节结果中获取书号
		err := s.node.BatchSaveChapters(bookNumber, chapters)
		if err != nil {
			fmt.Printf("[%s] 🔴 异步批量保存失败: %v\n", s.node.NodeID, err)
		} else {
			fmt.Printf("[%s] 💾 异步保存 %d 章完成\n", s.node.NodeID, len(chapters))
		}
	}
}

// Save 异步保存章节
func (s *AsyncDBSaver) Save(chapters []ChapterResult) {
	if len(chapters) == 0 {
		return
	}

	// 创建章节副本避免数据竞争
	chaptersCopy := make([]ChapterResult, len(chapters))
	copy(chaptersCopy, chapters)

	select {
	case s.saveChan <- chaptersCopy:
		// 成功发送到保存队列
	default:
		// 队列满了，同步保存（备用策略）
		fmt.Printf("[%s] ⚠️  异步队列满，同步保存 %d 章\n", s.node.NodeID, len(chapters))
		if len(chapters) > 0 {
			s.node.BatchSaveChapters(chapters[0].BookNumber, chapters)
		}
	}
}

// Close 关闭异步保存器
func (s *AsyncDBSaver) Close() {
	close(s.saveChan)
	s.wg.Wait()
}

// ProcessBook 处理整本书 - Worker池高带宽版本
func (node *CrawlerNode) ProcessBook(book *Book) error {
	fmt.Printf("[%s] 📖 开始处理书籍 %s (共%d章)\n", node.NodeID, book.BookNumber, book.ChapterCount)

	// 创建异步数据库保存器
	dbSaver := NewAsyncDBSaver(node)
	defer dbSaver.Close()

	// 结果收集channel
	resultChan := make(chan ChapterResult, book.ChapterCount)

	// 进度统计
	var (
		totalCompleted int32
		totalSuccess   int32
		totalFailed    int32
	)

	// 启动进度监控协程 - 减少刷屏频率
	progressTicker := time.NewTicker(4 * time.Second) // 每4秒打印一次
	defer progressTicker.Stop()

	var lastCompleted int32
	var lastPrintTime time.Time
	stuckCount := 0

	go func() {
		for range progressTicker.C {
			completed := atomic.LoadInt32(&totalCompleted)
			success := atomic.LoadInt32(&totalSuccess)
			failed := atomic.LoadInt32(&totalFailed)

			// 检测进度是否卡住
			if completed == lastCompleted && completed > 0 {
				stuckCount++
				if stuckCount >= 2 { // 8秒没进度报警
					fmt.Printf("[%s] ⚠️  检测到慢任务阻塞，当前: %d/%d\n",
						node.NodeID, completed, book.ChapterCount)
				}
			} else {
				stuckCount = 0
			}

			// 避免重复刷屏：只在进度有变化或超过5秒未打印时输出
			now := time.Now()
			shouldPrint := completed != lastCompleted || now.Sub(lastPrintTime) > 5*time.Second

			if completed > 0 && shouldPrint {
				successRate := float64(success) / float64(completed) * 100
				progress := float64(completed) / float64(book.ChapterCount) * 100

				fmt.Printf("[%s] 📊 进度: %d/%d (%.1f%%) 成功:%d 失败:%d 成功率:%.1f%%\n",
					node.NodeID, completed, book.ChapterCount, progress, success, failed, successRate)

				// 显示连接状态 (每10秒显示一次详细信息)
				if now.Sub(lastPrintTime) > 10*time.Second {
					fmt.Printf("[%s] 🌐 连接状态: 全部直连模式\n", node.NodeID)
				}

				lastPrintTime = now
			}

			lastCompleted = completed
		}
	}()

	// 使用Worker池分发任务 - 保守模式 + 中断恢复
	baseURL := "https://www.a9db770f8.lol/book/" + book.BookNumber + "/"
	fmt.Printf("[%s] 🚀 分发 %d 个任务到100个Worker池 (保守模式，更稳定)...\n", node.NodeID, book.ChapterCount)

	// 检查已完成的章节，避免重复爬取
	completedChapters := make(map[int]bool)
	checkQuery := `SELECT chapter_order FROM chapters WHERE book_id = ?`
	rows, err := node.DB.Query(checkQuery, book.BookNumber)
	if err == nil {
		for rows.Next() {
			var chapterOrder int
			if rows.Scan(&chapterOrder) == nil {
				completedChapters[chapterOrder] = true
			}
		}
		rows.Close()
		fmt.Printf("[%s] 📋 发现已完成章节: %d 个，跳过重复爬取\n", node.NodeID, len(completedChapters))
	}

	// 快速分发未完成的章节任务到worker池
	go func() {
		for chapterNum := 1; chapterNum <= book.ChapterCount; chapterNum++ {
			// 跳过已完成的章节
			if completedChapters[chapterNum] {
				// 发送一个成功结果，保证计数正确
				go func(num int) {
					resultChan <- ChapterResult{
						BookNumber: book.BookNumber,
						ChapterNum: num,
						Title:      fmt.Sprintf("第%d章", num),
						Content:    "已存在", // 标记为已存在
						Error:      nil,
					}
				}(chapterNum)
				continue
			}

			job := CrawlJob{
				BookNumber: book.BookNumber,
				ChapterNum: chapterNum,
				URL:        baseURL + fmt.Sprintf("%d.html", chapterNum),
				ResultChan: resultChan,
			}

			// 快速非阻塞发送
			select {
			case node.jobQueue <- job:
				// 任务成功加入队列
			default:
				// 队列满了稍等片刻
				time.Sleep(1 * time.Millisecond)
				node.jobQueue <- job
			}
		}
	}()

	// 收集结果
	var chapters []ChapterResult
	successCount := 0
	failedCount := 0

	// 合理超时设置 - 给足时间完成，但避免无限等待
	timeoutDuration := time.Duration(book.ChapterCount/50+30) * time.Second // 增加超时时间
	if timeoutDuration > 300*time.Second {
		timeoutDuration = 300 * time.Second // 最多5分钟
	}
	timeout := time.NewTimer(timeoutDuration)
	defer timeout.Stop()

	for i := 0; i < book.ChapterCount; i++ {
		select {
		case result := <-resultChan:
			atomic.AddInt32(&totalCompleted, 1)

			if result.Error != nil {
				atomic.AddInt32(&totalFailed, 1)
				failedCount++
				if failedCount <= 20 { // 允许更多错误日志
					fmt.Printf("[%s] ❌ 章节 %s-%d: %v\n", node.NodeID, book.BookNumber, result.ChapterNum, result.Error)
				}

				// 记录失败章节，包含错误信息
				url := baseURL + fmt.Sprintf("%d.html", result.ChapterNum)
				node.LogFailedChapter(book.BookNumber, result.ChapterNum, url, result.Error.Error())
				continue
			}

			// 处理已存在的章节
			if result.Content == "已存在" {
				atomic.AddInt32(&totalSuccess, 1)
				successCount++
				continue
			}

			if result.Content == "" {
				atomic.AddInt32(&totalFailed, 1)
				failedCount++
				url := baseURL + fmt.Sprintf("%d.html", result.ChapterNum)
				node.LogFailedChapter(book.BookNumber, result.ChapterNum, url, "章节内容为空")
				continue
			}

			atomic.AddInt32(&totalSuccess, 1)
			chapters = append(chapters, result)
			successCount++

			// 自适应批量保存 - 根据章节数自动调整批次大小
			batchSize := 50
			if book.ChapterCount > 1000 {
				batchSize = 100 // 大书用更大批次
			} else if book.ChapterCount < 200 {
				batchSize = 20 // 小书用小批次
			}

			if len(chapters) >= batchSize {
				dbSaver.Save(chapters)
				chapters = chapters[:0]
			}

		case <-timeout.C:
			fmt.Printf("[%s] ⏰ 书籍 %s 处理超时，已完成 %d/%d 章\n",
				node.NodeID, book.BookNumber, i, book.ChapterCount)
			goto finish
		}
	}

finish:
	// 保存剩余章节 - 关键！确保中断时不丢失数据
	if len(chapters) > 0 {
		fmt.Printf("[%s] 💾 保存剩余 %d 章节...\n", node.NodeID, len(chapters))
		dbSaver.Save(chapters)
	}

	fmt.Printf("[%s] 📥 等待数据库保存完成...\n", node.NodeID)

	fmt.Printf("[%s] ✅ 书籍 %s 处理完成: %d/%d 章成功 (成功率: %.1f%%)\n",
		node.NodeID, book.BookNumber, successCount, book.ChapterCount,
		float64(successCount)/float64(book.ChapterCount)*100)

	return nil
}

// RetryFailedChapters 智能补爬失败的章节
func (node *CrawlerNode) RetryFailedChapters() error {
	// 获取失败的章节，优先补爬重试次数少的、时间较早的章节
	// 跳过重试次数过多的章节（可能是永久性错误）
	query := `SELECT book_number, chapter_number, url, retry_count, error_message 
			  FROM failed_chapters 
			  WHERE retry_count <= 10 
			  ORDER BY retry_count ASC, failed_at ASC 
			  LIMIT 50` // 减少批次大小，避免阻塞过久

	rows, err := node.DB.Query(query)
	if err != nil {
		return fmt.Errorf("查询失败章节失败: %v", err)
	}
	defer rows.Close()

	var successCount, skipCount int

	for rows.Next() {
		// 检查是否需要关闭
		if node.IsShutdown() {
			fmt.Printf("[%s] 🛑 补爬中断，爬虫正在关闭\n", node.NodeID)
			break
		}

		var bookNumber, url, errorMsg string
		var chapterNum, retryCount int

		err = rows.Scan(&bookNumber, &chapterNum, &url, &retryCount, &errorMsg)
		if err != nil {
			continue
		}

		// 根据之前的错误类型决定是否跳过
		if strings.Contains(errorMsg, "章节不存在 (404)") {
			// 404错误说明章节真的不存在，跳过
			skipCount++
			fmt.Printf("[%s] ⏭️  跳过不存在的章节: %s-%d (404)\n", node.NodeID, bookNumber, chapterNum)
			continue
		}

		// 根据重试次数增加延迟，避免频繁重试
		if retryCount > 1 {
			delay := time.Duration(retryCount) * 2 * time.Second // 2s, 4s, 6s, 8s...
			fmt.Printf("[%s] ⏳ 章节 %s-%d 已重试 %d 次，等待 %v 后补爬\n",
				node.NodeID, bookNumber, chapterNum, retryCount, delay)
			time.Sleep(delay)
		}

		// 尝试重新爬取
		fmt.Printf("[%s] 🔄 补爬章节 %s-%d (第 %d 次尝试)\n", node.NodeID, bookNumber, chapterNum, retryCount+1)
		title, content, err := node.crawlChapterOptimized(url, bookNumber, chapterNum)

		if err == nil && content != "" {
			// 成功了，保存并删除失败记录
			err = node.SaveChapter(bookNumber, title, content, chapterNum)
			if err == nil {
				// 删除失败记录
				deleteQuery := `DELETE FROM failed_chapters WHERE book_number = ? AND chapter_number = ?`
				node.DB.Exec(deleteQuery, bookNumber, chapterNum)
				successCount++
				fmt.Printf("[%s] ✅ 补爬成功: %s-%d\n", node.NodeID, bookNumber, chapterNum)
			} else {
				// 保存到数据库失败，更新失败记录
				node.LogFailedChapter(bookNumber, chapterNum, url, fmt.Sprintf("保存失败: %v", err))
			}
		} else {
			// 补爬仍然失败，更新失败记录
			if err != nil {
				node.LogFailedChapter(bookNumber, chapterNum, url, err.Error())
			} else {
				node.LogFailedChapter(bookNumber, chapterNum, url, "补爬后内容仍为空")
			}
		}

		// 补爬间隔，避免过于频繁
		time.Sleep(100 * time.Millisecond)
	}

	if successCount > 0 || skipCount > 0 {
		fmt.Printf("[%s] 📊 补爬完成: 成功 %d 个，跳过 %d 个章节\n", node.NodeID, successCount, skipCount)
	} else {
		fmt.Printf("[%s] 📭 没有需要补爬的章节\n", node.NodeID)
	}

	return nil
}

// MarkBookCompleted 标记书籍完成
func (node *CrawlerNode) MarkBookCompleted(bookNumber string, success bool) error {
	status := 2 // 成功
	if !success {
		status = 3 // 失败
	}

	query := `UPDATE books SET crawl_status = ? WHERE book_number = ?`
	_, err := node.DB.Exec(query, status, bookNumber)
	return err
}

// SetShutdown 设置优雅关闭标志
func (node *CrawlerNode) SetShutdown() {
	atomic.StoreInt32(&node.shutdown, 1)
	fmt.Printf("[%s] 📝 已设置关闭标志，等待当前任务完成...\n", node.NodeID)
}

// IsShutdown 检查是否需要关闭
func (node *CrawlerNode) IsShutdown() bool {
	return atomic.LoadInt32(&node.shutdown) == 1
}

// Run 运行爬虫节点
func (node *CrawlerNode) Run() {
	fmt.Printf("[%s] 爬虫节点启动\n", node.NodeID)

	// 显示连接配置信息
	fmt.Printf("[%s] 🌐 连接配置: 全部直连模式\n", node.NodeID)

	for {
		// 检查关闭标志
		if node.IsShutdown() {
			fmt.Printf("[%s] 🛑 收到关闭信号，停止处理新任务\n", node.NodeID)
			break
		}

		// 获取任务
		book, err := node.GetNextTask()
		if err != nil {
			fmt.Printf("[%s] 获取任务失败: %v\n", node.NodeID, err)
			time.Sleep(5 * time.Second)
			continue
		}

		if book == nil {
			fmt.Printf("[%s] 没有更多任务，开始补爬失败章节\n", node.NodeID)
			if !node.IsShutdown() { // 关闭时不执行补爬
				node.RetryFailedChapters()
			}
			time.Sleep(10 * time.Second)
			continue
		}

		// 再次检查关闭标志（处理任务前）
		if node.IsShutdown() {
			fmt.Printf("[%s] 🛑 关闭信号检测到，跳过书籍 %s\n", node.NodeID, book.BookNumber)
			break
		}

		// 处理任务
		fmt.Printf("[%s] 📖 开始处理书籍 %s (支持中断恢复)\n", node.NodeID, book.BookNumber)
		err = node.ProcessBook(book)
		success := err == nil

		// 更新任务状态
		err = node.MarkBookCompleted(book.BookNumber, success)
		if err != nil {
			fmt.Printf("[%s] 更新任务状态失败: %v\n", node.NodeID, err)
		}

		if success {
			fmt.Printf("[%s] ✅ 书籍 %s 处理完成\n", node.NodeID, book.BookNumber)
		} else {
			fmt.Printf("[%s] ❌ 书籍 %s 处理失败\n", node.NodeID, book.BookNumber)
		}

		// 处理完一本书后再次检查关闭标志
		if node.IsShutdown() {
			fmt.Printf("[%s] 🛑 处理完当前书籍，准备安全退出\n", node.NodeID)
			break
		}
	}

	fmt.Printf("[%s] 🏁 爬虫节点已安全关闭\n", node.NodeID)
}

func main() {
	// 生成节点ID（可以用主机名+进程ID）
	hostname, _ := os.Hostname()
	nodeID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	node, err := NewCrawlerNode(nodeID)
	if err != nil {
		fmt.Printf("创建爬虫节点失败: %v\n", err)
		return
	}
	defer node.DB.Close()

	// 设置信号处理 - 优雅关闭
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 启动爬虫节点（异步）
	done := make(chan bool)
	go func() {
		node.Run()
		done <- true
	}()

	// 等待信号或程序结束
	select {
	case <-sigChan:
		fmt.Printf("\n[%s] 🛑 接收到中断信号，开始优雅关闭...\n", nodeID)
		node.SetShutdown() // 设置关闭标志

		// 给程序10秒时间完成当前任务
		select {
		case <-done:
			fmt.Printf("[%s] ✅ 程序正常结束\n", nodeID)
		case <-time.After(10 * time.Second):
			fmt.Printf("[%s] ⏰ 超时强制退出\n", nodeID)
		}
	case <-done:
		fmt.Printf("[%s] ✅ 程序正常结束\n", nodeID)
	}
}
