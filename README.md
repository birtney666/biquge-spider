# 笔趣阁爬虫配置管理

## 配置文件说明

项目现已支持统一的配置文件管理，所有静态配置都可以通过 `config.yaml` 文件进行设置。

### 配置文件位置

- 主配置文件：`go/config.yaml`
- 示例配置文件：`go/config.example.yaml`

### 快速开始

1. 复制示例配置文件：
```bash
cd go
cp config.example.yaml config.yaml
```

2. 根据你的环境修改配置文件：
```yaml
# 修改数据库连接信息
database:
  dsn: "root:你的密码@tcp(localhost:3306)/novel?charset=utf8mb4&parseTime=True&loc=Local&maxAllowedPacket=0"

# 如需更换目标网站，修改站点配置
site:
  base_url: "https://your-target-site.com"
  book_url_template: "https://your-target-site.com/book/{book_number}/"
  chapter_url_template: "https://your-target-site.com/book/{book_number}/{chapter_num}.html"
```

### 配置项说明

#### 应用配置 (app)
- `name`: 应用名称
- `version`: 应用版本

#### 站点配置 (site)
- `base_url`: 目标网站基础URL
- `book_url_template`: 书籍URL模板，`{book_number}` 会被替换为书籍编号
- `chapter_url_template`: 章节URL模板，支持 `{book_number}` 和 `{chapter_num}` 占位符

#### 数据库配置 (database)
- `dsn`: 数据库连接字符串
- `max_open_conns`: 最大打开连接数
- `max_idle_conns`: 最大空闲连接数
- `conn_max_lifetime_minutes`: 连接最大生存时间（分钟）

#### HTTP客户端配置 (http)
- `timeout_seconds`: 请求超时时间（秒）
- `max_idle_conns`: 最大空闲连接数
- `max_idle_conns_per_host`: 每个主机最大空闲连接数
- `max_conns_per_host`: 每个主机最大连接数
- `idle_conn_timeout_seconds`: 空闲连接超时时间（秒）
- `dial_timeout_seconds`: 拨号超时时间（秒）
- `keep_alive_duration_seconds`: 保持活动持续时间（秒）
- `disable_compression`: 是否禁用压缩
- `response_header_timeout_seconds`: 响应头超时时间（秒）
- `expect_continue_timeout_seconds`: Expect Continue超时时间（秒）
- `insecure_skip_verify`: 是否跳过SSL验证
- `force_http2`: 是否强制使用HTTP/2

#### 爬虫配置 (crawler)
- `num_workers`: 并发工作协程数量
- `job_queue_size`: 任务队列大小
- `rate_limit`: 限流配置（每秒请求数）
- `rate_burst`: 突发请求数量
- `max_retries`: 最大重试次数
- `base_delay_seconds`: 基础延迟时间（秒）
- `batch_size`: 批次保存大小
- `progress_ticker_seconds`: 进度报告间隔（秒）

### 配置文件加载逻辑

1. 程序启动时首先尝试加载当前目录下的 `config.yaml`
2. 如果配置文件加载失败，程序会自动使用内置的默认配置
3. 程序会自动将配置中的秒数转换为Go的 `time.Duration` 类型

### 运行程序

```bash
cd go
go run .
```

程序会自动加载配置文件并显示加载状态。

### 测试配置

运行配置测试来验证配置文件功能：

```bash
cd go
go test -v -run TestConfig
```

### 注意事项

1. **敏感信息安全**：请不要将包含真实密码的 `config.yaml` 文件提交到版本控制系统
2. **配置验证**：程序启动时会验证配置文件格式，如有错误会显示详细信息
3. **动态更新**：目前配置文件只在程序启动时加载，修改配置后需要重启程序
4. **向后兼容**：如果不使用配置文件，程序仍能使用默认配置正常运行

### 配置迁移

从硬编码配置迁移到配置文件：

| 原硬编码位置 | 新配置项 |
|------------|---------|
| config.go:56 数据库DSN | database.dsn |
| client.go:240 章节URL | site.chapter_url_template |
| crawler.go:185 书籍URL | site.book_url_template |
| config.go:57-59 数据库连接池 | database.* |
| config.go:61-74 HTTP客户端 | http.* |
| config.go:75-84 爬虫参数 | crawler.* |