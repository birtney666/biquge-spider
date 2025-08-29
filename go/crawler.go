package main

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// CrawlJob 爬取任务
type CrawlJob struct {
	BookNumber string
	ChapterNum int
	URL        string
	ResultChan chan ChapterResult
}

// CrawlerNode 爬虫节点
type CrawlerNode struct {
	NodeID   string
	Database *Database
	Client   *HTTPClient
	Config   *Config

	hostLimiter *rate.Limiter
	workerPool  chan struct{}
	jobQueue    chan CrawlJob
	shutdown    int32
}

// NewCrawlerNode 创建爬虫节点
func NewCrawlerNode(nodeID string, config *Config) (*CrawlerNode, error) {
	db, err := NewDatabase(config.Database)
	if err != nil {
		return nil, err
	}

	client := NewHTTPClient(config.HTTP)
	hostLimiter := rate.NewLimiter(rate.Limit(config.Crawler.RateLimit), config.Crawler.RateBurst)

	workerPool := make(chan struct{}, config.Crawler.NumWorkers)
	jobQueue := make(chan CrawlJob, config.Crawler.JobQueueSize)

	node := &CrawlerNode{
		NodeID:      nodeID,
		Database:    db,
		Client:      client,
		Config:      config,
		hostLimiter: hostLimiter,
		workerPool:  workerPool,
		jobQueue:    jobQueue,
	}

	for i := 0; i < config.Crawler.NumWorkers; i++ {
		go node.crawlWorker()
	}

	return node, nil
}

// crawlWorker 爬虫工作协程
func (node *CrawlerNode) crawlWorker() {
	for job := range node.jobQueue {
		node.hostLimiter.Wait(context.Background())

		title, content, err := node.Client.FetchChapter(
			job.URL,
			job.BookNumber,
			job.ChapterNum,
			node.Config.Crawler.MaxRetries,
			node.Config.Crawler.BaseDelay,
		)

		job.ResultChan <- ChapterResult{
			BookNumber: job.BookNumber,
			ChapterNum: job.ChapterNum,
			Title:      title,
			Content:    content,
			Error:      err,
		}
	}
}

// ProcessBook 处理整本书
func (node *CrawlerNode) ProcessBook(book *Book) error {
	fmt.Printf("[%s] 📖 开始处理书籍 %s (共%d章)\n", node.NodeID, book.BookNumber, book.ChapterCount)

	dbSaver := NewAsyncDBSaver(node)
	defer dbSaver.Close()

	resultChan := make(chan ChapterResult, book.ChapterCount)

	var (
		totalCompleted int32
		totalSuccess   int32
		totalFailed    int32
	)

	node.startProgressMonitor(book.ChapterCount, &totalCompleted, &totalSuccess, &totalFailed)

	completedChapters, err := node.Database.GetCompletedChapters(book.BookNumber)
	if err == nil {
		fmt.Printf("[%s] 📋 发现已完成章节: %d 个，跳过重复爬取\n", node.NodeID, len(completedChapters))
	}

	go node.dispatchJobs(book, completedChapters, resultChan)

	var chapters []ChapterResult
	successCount := 0
	failedCount := 0

	timeoutDuration := time.Duration(book.ChapterCount/50+30) * time.Second
	if timeoutDuration > 300*time.Second {
		timeoutDuration = 300 * time.Second
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
				if failedCount <= 20 {
					fmt.Printf("[%s] ❌ 章节 %s-%d: %v\n", node.NodeID, book.BookNumber, result.ChapterNum, result.Error)
				}

				url := node.Config.GetChapterURL(book.BookNumber, result.ChapterNum)
				node.Database.LogFailedChapter(book.BookNumber, result.ChapterNum, url, result.Error.Error())
				continue
			}

			if result.Content == "已存在" {
				atomic.AddInt32(&totalSuccess, 1)
				successCount++
				continue
			}

			if result.Content == "" {
				atomic.AddInt32(&totalFailed, 1)
				failedCount++
				url := node.Config.GetChapterURL(book.BookNumber, result.ChapterNum)
				node.Database.LogFailedChapter(book.BookNumber, result.ChapterNum, url, "章节内容为空")
				continue
			}

			atomic.AddInt32(&totalSuccess, 1)
			chapters = append(chapters, result)
			successCount++

			batchSize := node.getBatchSize(book.ChapterCount)
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
	if len(chapters) > 0 {
		fmt.Printf("[%s] 💾 保存剩余 %d 章节...\n", node.NodeID, len(chapters))
		dbSaver.Save(chapters)
	}

	fmt.Printf("[%s] ✅ 书籍 %s 处理完成: %d/%d 章成功 (成功率: %.1f%%)\n",
		node.NodeID, book.BookNumber, successCount, book.ChapterCount,
		float64(successCount)/float64(book.ChapterCount)*100)

	return nil
}

// dispatchJobs 分发任务
func (node *CrawlerNode) dispatchJobs(book *Book, completedChapters map[int]bool, resultChan chan ChapterResult) {
	for chapterNum := 1; chapterNum <= book.ChapterCount; chapterNum++ {
		if completedChapters[chapterNum] {
			go func(num int) {
				resultChan <- ChapterResult{
					BookNumber: book.BookNumber,
					ChapterNum: num,
					Title:      fmt.Sprintf("第%d章", num),
					Content:    "已存在",
					Error:      nil,
				}
			}(chapterNum)
			continue
		}

		job := CrawlJob{
			BookNumber: book.BookNumber,
			ChapterNum: chapterNum,
			URL:        node.Config.GetChapterURL(book.BookNumber, chapterNum),
			ResultChan: resultChan,
		}

		select {
		case node.jobQueue <- job:
		default:
			time.Sleep(1 * time.Millisecond)
			node.jobQueue <- job
		}
	}
}

// getBatchSize 获取批次大小
func (node *CrawlerNode) getBatchSize(chapterCount int) int {
	batchSize := node.Config.Crawler.BatchSize
	if chapterCount > 1000 {
		batchSize = 100
	} else if chapterCount < 200 {
		batchSize = 20
	}
	return batchSize
}

// startProgressMonitor 启动进度监控
func (node *CrawlerNode) startProgressMonitor(totalChapters int, completed, success, failed *int32) {
	progressTicker := time.NewTicker(node.Config.Crawler.ProgressTicker)

	var lastCompleted int32
	var lastPrintTime time.Time
	stuckCount := 0

	go func() {
		defer progressTicker.Stop()
		for range progressTicker.C {
			current := atomic.LoadInt32(completed)
			successCount := atomic.LoadInt32(success)
			failedCount := atomic.LoadInt32(failed)

			if current == lastCompleted && current > 0 {
				stuckCount++
				if stuckCount >= 2 {
					fmt.Printf("[%s] ⚠️  检测到慢任务阻塞，当前: %d/%d\n",
						node.NodeID, current, totalChapters)
				}
			} else {
				stuckCount = 0
			}

			now := time.Now()
			shouldPrint := current != lastCompleted || now.Sub(lastPrintTime) > 5*time.Second

			if current > 0 && shouldPrint {
				successRate := float64(successCount) / float64(current) * 100
				progress := float64(current) / float64(totalChapters) * 100

				fmt.Printf("[%s] 📊 进度: %d/%d (%.1f%%) 成功:%d 失败:%d 成功率:%.1f%%\n",
					node.NodeID, current, totalChapters, progress, successCount, failedCount, successRate)

				lastPrintTime = now
			}

			lastCompleted = current

			if current >= int32(totalChapters) {
				break
			}
		}
	}()
}

// RetryFailedChapters 智能补爬失败的章节
func (node *CrawlerNode) RetryFailedChapters() error {
	failures, err := node.Database.GetFailedChapters(50)
	if err != nil {
		return err
	}

	var successCount, skipCount int

	for _, failure := range failures {
		if node.IsShutdown() {
			fmt.Printf("[%s] 🛑 补爬中断，爬虫正在关闭\n", node.NodeID)
			break
		}

		if strings.Contains(failure.ErrorMsg, "章节不存在 (404)") {
			skipCount++
			fmt.Printf("[%s] ⏭️  跳过不存在的章节: %s-%d (404)\n",
				node.NodeID, failure.BookNumber, failure.ChapterNum)
			continue
		}

		if failure.RetryCount > 1 {
			delay := time.Duration(failure.RetryCount) * 2 * time.Second
			time.Sleep(delay)
		}

		fmt.Printf("[%s] 🔄 补爬章节 %s-%d (第 %d 次尝试)\n",
			node.NodeID, failure.BookNumber, failure.ChapterNum, failure.RetryCount+1)

		title, content, err := node.Client.FetchChapter(
			failure.URL,
			failure.BookNumber,
			failure.ChapterNum,
			node.Config.Crawler.MaxRetries,
			node.Config.Crawler.BaseDelay,
		)

		if err == nil && content != "" {
			err = node.Database.SaveChapter(failure.BookNumber, title, content, failure.ChapterNum)
			if err == nil {
				node.Database.RemoveFailedChapter(failure.BookNumber, failure.ChapterNum)
				successCount++
				fmt.Printf("[%s] ✅ 补爬成功: %s-%d\n", node.NodeID, failure.BookNumber, failure.ChapterNum)
			} else {
				node.Database.LogFailedChapter(failure.BookNumber, failure.ChapterNum, failure.URL,
					fmt.Sprintf("保存失败: %v", err))
			}
		} else {
			if err != nil {
				node.Database.LogFailedChapter(failure.BookNumber, failure.ChapterNum, failure.URL, err.Error())
			} else {
				node.Database.LogFailedChapter(failure.BookNumber, failure.ChapterNum, failure.URL, "补爬后内容仍为空")
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

	if successCount > 0 || skipCount > 0 {
		fmt.Printf("[%s] 📊 补爬完成: 成功 %d 个，跳过 %d 个章节\n", node.NodeID, successCount, skipCount)
	} else {
		fmt.Printf("[%s] 📭 没有需要补爬的章节\n", node.NodeID)
	}

	return nil
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
	fmt.Printf("[%s] 🌐 连接配置: 全部直连模式\n", node.NodeID)

	for {
		if node.IsShutdown() {
			fmt.Printf("[%s] 🛑 收到关闭信号，停止处理新任务\n", node.NodeID)
			break
		}

		book, err := node.Database.GetNextTask()
		if err != nil {
			fmt.Printf("[%s] 获取任务失败: %v\n", node.NodeID, err)
			time.Sleep(5 * time.Second)
			continue
		}

		if book == nil {
			fmt.Printf("[%s] 没有更多任务，开始补爬失败章节\n", node.NodeID)
			if !node.IsShutdown() {
				node.RetryFailedChapters()
			}
			time.Sleep(10 * time.Second)
			continue
		}

		if node.IsShutdown() {
			fmt.Printf("[%s] 🛑 关闭信号检测到，跳过书籍 %s\n", node.NodeID, book.BookNumber)
			break
		}

		fmt.Printf("[%s] 📖 开始处理书籍 %s (支持中断恢复)\n", node.NodeID, book.BookNumber)
		err = node.ProcessBook(book)
		success := err == nil

		err = node.Database.MarkBookCompleted(book.BookNumber, success)
		if err != nil {
			fmt.Printf("[%s] 更新任务状态失败: %v\n", node.NodeID, err)
		}

		if success {
			fmt.Printf("[%s] ✅ 书籍 %s 处理完成\n", node.NodeID, book.BookNumber)
		} else {
			fmt.Printf("[%s] ❌ 书籍 %s 处理失败\n", node.NodeID, book.BookNumber)
		}

		if node.IsShutdown() {
			fmt.Printf("[%s] 🛑 处理完当前书籍，准备安全退出\n", node.NodeID)
			break
		}
	}

	fmt.Printf("[%s] 🏁 爬虫节点已安全关闭\n", node.NodeID)
}

// Close 关闭爬虫节点
func (node *CrawlerNode) Close() error {
	close(node.jobQueue)
	return node.Database.Close()
}
