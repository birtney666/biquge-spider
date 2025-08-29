package main

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

// Book 书籍信息结构
type Book struct {
	BookNumber   string
	ChapterCount int
}

// ChapterResult 章节结果结构
type ChapterResult struct {
	BookNumber string
	ChapterNum int
	Title      string
	Content    string
	Error      error
}

// Database 数据库操作结构
type Database struct {
	db     *sql.DB
	config DatabaseConfig
}

// NewDatabase 创建数据库连接
func NewDatabase(config DatabaseConfig) (*Database, error) {
	db, err := sql.Open("mysql", config.DSN)
	if err != nil {
		return nil, fmt.Errorf("数据库连接失败: %v", err)
	}

	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("数据库连接测试失败: %v", err)
	}

	return &Database{
		db:     db,
		config: config,
	}, nil
}

// GetNextTask 使用SELECT FOR UPDATE获取下一个任务
func (d *Database) GetNextTask() (*Book, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("开始事务失败: %v", err)
	}
	defer tx.Rollback()

	query := `SELECT book_number, chapter_count 
			  FROM books 
			  WHERE crawl_status = 0 
			  ORDER BY CAST(book_number AS UNSIGNED) ASC 
			  LIMIT 1 FOR UPDATE`

	var book Book
	err = tx.QueryRow(query).Scan(&book.BookNumber, &book.ChapterCount)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("查询任务失败: %v", err)
	}

	updateQuery := `UPDATE books SET crawl_status = 1 WHERE book_number = ?`
	_, err = tx.Exec(updateQuery, book.BookNumber)
	if err != nil {
		return nil, fmt.Errorf("更新任务状态失败: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("提交事务失败: %v", err)
	}

	return &book, nil
}

// BatchSaveChapters 批量保存章节到数据库
func (d *Database) BatchSaveChapters(bookID string, chapters []ChapterResult) error {
	if len(chapters) == 0 {
		return nil
	}

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

	_, err := d.db.Exec(query, values...)
	return err
}

// SaveChapter 保存单个章节到数据库
func (d *Database) SaveChapter(bookID string, chapterTitle string, content string, chapterOrder int) error {
	query := `INSERT INTO chapters (book_id, chapter_name, chapter_order, content) 
			  VALUES (?, ?, ?, ?) 
			  ON DUPLICATE KEY UPDATE content = VALUES(content)`

	_, err := d.db.Exec(query, bookID, chapterTitle, chapterOrder, content)
	return err
}

// LogFailedChapter 记录失败章节用于补爬
func (d *Database) LogFailedChapter(bookNumber string, chapterNum int, url string, errorMsg string) error {
	checkQuery := `SELECT retry_count FROM failed_chapters WHERE book_number = ? AND chapter_number = ?`
	var retryCount int
	err := d.db.QueryRow(checkQuery, bookNumber, chapterNum).Scan(&retryCount)

	if err == sql.ErrNoRows {
		query := `INSERT INTO failed_chapters (book_number, chapter_number, url, error_message, retry_count, failed_at, last_retry_at) 
				  VALUES (?, ?, ?, ?, 1, NOW(), NOW())`
		_, err = d.db.Exec(query, bookNumber, chapterNum, url, errorMsg)
		return err
	} else if err == nil {
		updateQuery := `UPDATE failed_chapters 
						SET retry_count = retry_count + 1, 
							error_message = ?, 
							last_retry_at = NOW() 
						WHERE book_number = ? AND chapter_number = ?`
		_, err = d.db.Exec(updateQuery, errorMsg, bookNumber, chapterNum)
		return err
	}

	return err
}

// GetCompletedChapters 获取已完成的章节
func (d *Database) GetCompletedChapters(bookNumber string) (map[int]bool, error) {
	completed := make(map[int]bool)
	checkQuery := `SELECT chapter_order FROM chapters WHERE book_id = ?`
	rows, err := d.db.Query(checkQuery, bookNumber)
	if err != nil {
		return completed, err
	}
	defer rows.Close()

	for rows.Next() {
		var chapterOrder int
		if rows.Scan(&chapterOrder) == nil {
			completed[chapterOrder] = true
		}
	}

	return completed, nil
}

// GetFailedChapters 获取失败的章节列表
func (d *Database) GetFailedChapters(limit int) ([]FailedChapter, error) {
	query := `SELECT book_number, chapter_number, url, retry_count, error_message 
			  FROM failed_chapters 
			  WHERE retry_count <= 10 
			  ORDER BY retry_count ASC, failed_at ASC 
			  LIMIT ?`

	rows, err := d.db.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("查询失败章节失败: %v", err)
	}
	defer rows.Close()

	var failures []FailedChapter
	for rows.Next() {
		var fc FailedChapter
		err = rows.Scan(&fc.BookNumber, &fc.ChapterNum, &fc.URL, &fc.RetryCount, &fc.ErrorMsg)
		if err != nil {
			continue
		}
		failures = append(failures, fc)
	}

	return failures, nil
}

// RemoveFailedChapter 删除失败章节记录
func (d *Database) RemoveFailedChapter(bookNumber string, chapterNum int) error {
	deleteQuery := `DELETE FROM failed_chapters WHERE book_number = ? AND chapter_number = ?`
	_, err := d.db.Exec(deleteQuery, bookNumber, chapterNum)
	return err
}

// MarkBookCompleted 标记书籍完成
func (d *Database) MarkBookCompleted(bookNumber string, success bool) error {
	status := 2
	if !success {
		status = 3
	}

	query := `UPDATE books SET crawl_status = ? WHERE book_number = ?`
	_, err := d.db.Exec(query, status, bookNumber)
	return err
}

// Close 关闭数据库连接
func (d *Database) Close() error {
	return d.db.Close()
}

// FailedChapter 失败章节结构
type FailedChapter struct {
	BookNumber string
	ChapterNum int
	URL        string
	RetryCount int
	ErrorMsg   string
}