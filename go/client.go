package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"golang.org/x/net/http2"
)

// HTTPClient HTTP客户端结构
type HTTPClient struct {
	client *http.Client
	config HTTPConfig
}

// CompiledRegex 预编译的正则表达式
var (
	titleRegex    = regexp.MustCompile(`<title>([^_]+)`)
	contentRegex1 = regexp.MustCompile(`(?s)<div[^>]*id=["']chaptercontent["'][^>]*>(.*?)</div>`)
	contentRegex2 = regexp.MustCompile(`(?s)<div[^>]*class=["'][^"']*content[^"']*["'][^>]*>(.*?)</div>`)
	contentRegex3 = regexp.MustCompile(`(?s)<div[^>]*>([^<]{100,}.*?)</div>`)
	htmlTagRegex  = regexp.MustCompile(`<[^>]*>`)
)

// NewHTTPClient 创建HTTP客户端
func NewHTTPClient(config HTTPConfig) *HTTPClient {
	transport := &http.Transport{
		MaxIdleConns:        config.MaxIdleConns,
		MaxIdleConnsPerHost: config.MaxIdleConnsPerHost,
		MaxConnsPerHost:     config.MaxConnsPerHost,
		IdleConnTimeout:     config.IdleConnTimeout,

		DialContext: (&net.Dialer{
			Timeout:   config.DialTimeout,
			KeepAlive: config.KeepAliveDuration,
		}).DialContext,

		DisableKeepAlives:     false,
		DisableCompression:    config.DisableCompression,
		ResponseHeaderTimeout: config.ResponseHeaderTimeout,
		ExpectContinueTimeout: config.ExpectContinueTimeout,

		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: config.InsecureSkipVerify,
		},

		ForceAttemptHTTP2: config.ForceHTTP2,
	}

	if config.ForceHTTP2 {
		http2.ConfigureTransport(transport)
	}

	client := &http.Client{
		Timeout:   config.Timeout,
		Transport: transport,
	}

	return &HTTPClient{
		client: client,
		config: config,
	}
}

// FetchChapter 爬取单个章节
func (c *HTTPClient) FetchChapter(url, bookNumber string, chapterNum int, maxRetries int, baseDelay time.Duration) (string, string, error) {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		timeout := time.Duration(5+attempt*2) * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			cancel()
			return "", "", fmt.Errorf("创建请求失败: %v", err)
		}

		c.setRequestHeaders(req)

		resp, err := c.client.Do(req)
		cancel()

		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				lastErr = fmt.Errorf("请求超时(%v)", timeout)
			} else {
				lastErr = fmt.Errorf("请求失败: %v", err)
			}

			if attempt < maxRetries {
				delay := baseDelay * time.Duration(1<<uint(attempt))
				time.Sleep(delay)
				continue
			}
			return "", "", lastErr
		}

		statusCode := resp.StatusCode
		if !c.isSuccessStatus(statusCode) {
			resp.Body.Close()
			delay := c.getRetryDelay(statusCode, attempt)
			if delay > 0 && attempt < maxRetries {
				time.Sleep(delay)
				continue
			}
			return "", "", fmt.Errorf("HTTP状态码: %d", statusCode)
		}

		limitedBody := http.MaxBytesReader(nil, resp.Body, 2*1024*1024)
		body, err := ioutil.ReadAll(limitedBody)
		resp.Body.Close()

		if err != nil {
			lastErr = fmt.Errorf("读取响应失败: %v", err)
			if attempt < maxRetries {
				delay := baseDelay * time.Duration(1+attempt)
				time.Sleep(delay)
				continue
			}
			return "", "", lastErr
		}

		html := string(body)

		if len(html) < 100 {
			lastErr = fmt.Errorf("页面内容过短 (%d 字符)", len(html))
			if attempt < maxRetries {
				delay := baseDelay * time.Duration(1+attempt)
				time.Sleep(delay)
				continue
			}
			return "", "", lastErr
		}

		title, content := c.extractContent(html, chapterNum)

		if content == "" {
			lastErr = fmt.Errorf("未找到章节内容")
			if attempt < maxRetries {
				delay := baseDelay * time.Duration(1+attempt)
				time.Sleep(delay)
				continue
			}
			return title, "", lastErr
		}

		if len(content) < 10 {
			lastErr = fmt.Errorf("章节内容过短 (%d 字符)", len(content))
			if attempt < maxRetries {
				delay := baseDelay * time.Duration(1+attempt)
				time.Sleep(delay)
				continue
			}
			return title, "", lastErr
		}

		return title, content, nil
	}

	return "", "", fmt.Errorf("重试 %d 次后失败: %v", maxRetries, lastErr)
}

// setRequestHeaders 设置请求头
func (c *HTTPClient) setRequestHeaders(req *http.Request) {
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Cache-Control", "no-cache")
}

// isSuccessStatus 检查HTTP状态码是否成功
func (c *HTTPClient) isSuccessStatus(statusCode int) bool {
	switch statusCode {
	case 200:
		return true
	case 404, 403, 429:
		return false
	case 500, 502, 503, 504:
		return false
	default:
		return false
	}
}

// getRetryDelay 根据状态码获取重试延迟
func (c *HTTPClient) getRetryDelay(statusCode, attempt int) time.Duration {
	switch statusCode {
	case 403:
		return time.Duration(10*(attempt+1)) * time.Second
	case 429:
		return time.Duration(15*(attempt+1)) * time.Second
	case 500, 502, 503, 504:
		return time.Duration(5*(attempt+1)) * time.Second
	default:
		return time.Duration(2*(attempt+1)) * time.Second
	}
}

// extractContent 从HTML中提取标题和内容
func (c *HTTPClient) extractContent(html string, chapterNum int) (string, string) {
	title := fmt.Sprintf("第%d章", chapterNum)
	if titleMatch := titleRegex.FindStringSubmatch(html); len(titleMatch) > 1 {
		title = strings.TrimSpace(titleMatch[1])
	}

	var content string
	if match := contentRegex1.FindStringSubmatch(html); len(match) > 1 {
		content = match[1]
	} else if match := contentRegex2.FindStringSubmatch(html); len(match) > 1 {
		content = match[1]
	} else if matches := contentRegex3.FindAllStringSubmatch(html, -1); len(matches) > 0 {
		for _, match := range matches {
			if len(match[1]) > len(content) {
				content = match[1]
			}
		}
	}

	content = htmlTagRegex.ReplaceAllString(content, "")
	content = strings.ReplaceAll(content, "&nbsp;", " ")
	content = strings.ReplaceAll(content, "&#160;", " ")
	content = strings.ReplaceAll(content, "\r", "")
	content = strings.ReplaceAll(content, "\n\n", "\n")
	content = strings.TrimSpace(content)

	return title, content
}

