package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// AppConfig 应用配置
type AppConfig struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
}

// SiteConfig 网站配置
type SiteConfig struct {
	BaseURL            string `yaml:"base_url"`
	BookURLTemplate    string `yaml:"book_url_template"`
	ChapterURLTemplate string `yaml:"chapter_url_template"`
}

// Config 爬虫配置
type Config struct {
	App      AppConfig      `yaml:"app"`
	Site     SiteConfig     `yaml:"site"`
	Database DatabaseConfig `yaml:"database"`
	HTTP     HTTPConfig     `yaml:"http"`
	Crawler  CrawlerConfig  `yaml:"crawler"`
}

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	DSN                   string        `yaml:"dsn"`
	MaxOpenConns          int           `yaml:"max_open_conns"`
	MaxIdleConns          int           `yaml:"max_idle_conns"`
	ConnMaxLifetimeMinutes int          `yaml:"conn_max_lifetime_minutes"`
	ConnMaxLifetime       time.Duration // 运行时使用
}

// HTTPConfig HTTP客户端配置
type HTTPConfig struct {
	TimeoutSeconds               int           `yaml:"timeout_seconds"`
	MaxIdleConns                 int           `yaml:"max_idle_conns"`
	MaxIdleConnsPerHost          int           `yaml:"max_idle_conns_per_host"`
	MaxConnsPerHost              int           `yaml:"max_conns_per_host"`
	IdleConnTimeoutSeconds       int           `yaml:"idle_conn_timeout_seconds"`
	DialTimeoutSeconds           int           `yaml:"dial_timeout_seconds"`
	KeepAliveDurationSeconds     int           `yaml:"keep_alive_duration_seconds"`
	DisableCompression           bool          `yaml:"disable_compression"`
	ResponseHeaderTimeoutSeconds int           `yaml:"response_header_timeout_seconds"`
	ExpectContinueTimeoutSeconds int           `yaml:"expect_continue_timeout_seconds"`
	InsecureSkipVerify           bool          `yaml:"insecure_skip_verify"`
	ForceHTTP2                   bool          `yaml:"force_http2"`
	// 运行时使用的时间字段
	Timeout               time.Duration
	IdleConnTimeout       time.Duration
	DialTimeout           time.Duration
	KeepAliveDuration     time.Duration
	ResponseHeaderTimeout time.Duration
	ExpectContinueTimeout time.Duration
}

// CrawlerConfig 爬虫配置
type CrawlerConfig struct {
	NumWorkers            int           `yaml:"num_workers"`
	JobQueueSize          int           `yaml:"job_queue_size"`
	RateLimit             int           `yaml:"rate_limit"`
	RateBurst             int           `yaml:"rate_burst"`
	MaxRetries            int           `yaml:"max_retries"`
	BaseDelaySeconds      int           `yaml:"base_delay_seconds"`
	BatchSize             int           `yaml:"batch_size"`
	ProgressTickerSeconds int           `yaml:"progress_ticker_seconds"`
	// 运行时使用的时间字段
	BaseDelay      time.Duration
	ProgressTicker time.Duration
}

// LoadConfig 从配置文件加载配置
func LoadConfig(configPath string) (*Config, error) {
	if configPath == "" {
		configPath = "config.yaml"
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %v", err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %v", err)
	}

	return &config, nil
}


// ToTimeConfig 将配置转换为实际使用的时间配置
func (c *Config) ToTimeConfig() *Config {
	c.Database.ConnMaxLifetime = time.Duration(c.Database.ConnMaxLifetimeMinutes) * time.Minute
	c.HTTP.Timeout = time.Duration(c.HTTP.TimeoutSeconds) * time.Second
	c.HTTP.IdleConnTimeout = time.Duration(c.HTTP.IdleConnTimeoutSeconds) * time.Second
	c.HTTP.DialTimeout = time.Duration(c.HTTP.DialTimeoutSeconds) * time.Second
	c.HTTP.KeepAliveDuration = time.Duration(c.HTTP.KeepAliveDurationSeconds) * time.Second
	c.HTTP.ResponseHeaderTimeout = time.Duration(c.HTTP.ResponseHeaderTimeoutSeconds) * time.Second
	c.HTTP.ExpectContinueTimeout = time.Duration(c.HTTP.ExpectContinueTimeoutSeconds) * time.Second
	c.Crawler.BaseDelay = time.Duration(c.Crawler.BaseDelaySeconds) * time.Second
	c.Crawler.ProgressTicker = time.Duration(c.Crawler.ProgressTickerSeconds) * time.Second
	return c
}

// GetChapterURL 根据配置生成章节URL
func (c *Config) GetChapterURL(bookNumber string, chapterNum int) string {
	url := c.Site.ChapterURLTemplate
	url = strings.ReplaceAll(url, "{book_number}", bookNumber)
	url = strings.ReplaceAll(url, "{chapter_num}", fmt.Sprintf("%d", chapterNum))
	return c.Site.BaseURL + url
}

// GetBookURL 根据配置生成书籍URL
func (c *Config) GetBookURL(bookNumber string) string {
	url := c.Site.BookURLTemplate
	url = strings.ReplaceAll(url, "{book_number}", bookNumber)
	return c.Site.BaseURL + url
}
