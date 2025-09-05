package main

import (
	"fmt"
	"sync"
)

// AsyncDBSaver 异步数据库保存器
type AsyncDBSaver struct {
	saveChan chan []ChapterResult
	database *Database
	wg       sync.WaitGroup
	nodeID   string
}

// NewAsyncDBSaver 创建异步数据库保存器
func NewAsyncDBSaver(node *CrawlerNode) *AsyncDBSaver {
	saver := &AsyncDBSaver{
		saveChan: make(chan []ChapterResult, 10),
		database: node.Database,
		nodeID:   node.NodeID,
	}

	saver.wg.Add(1)
	go saver.saveWorker()

	return saver
}

// saveWorker 后台保存工作协程2                    	``````````
func (s *AsyncDBSaver) saveWorker() {
	defer s.wg.Done()

	for chapters := range s.saveChan {
		if len(chapters) == 0 {
			continue
		}

		bookNumber := chapters[0].BookNumber
		err := s.database.BatchSaveChapters(bookNumber, chapters)
		if err != nil {
			fmt.Printf("[%s] 🔴 异步批量保存失败: %v\n", s.nodeID, err)
		} else {
			fmt.Printf("[%s] 💾 异步保存 %d 章完成\n", s.nodeID, len(chapters))
		}
	}
}

// Save 异步保存章节
func (s *AsyncDBSaver) Save(chapters []ChapterResult) {
	if len(chapters) == 0 {
		return
	}

	chaptersCopy := make([]ChapterResult, len(chapters))
	copy(chaptersCopy, chapters)

	select {
	case s.saveChan <- chaptersCopy:
	default:
		fmt.Printf("[%s] ⚠️  异步队列满，同步保存 %d 章\n", s.nodeID, len(chapters))
		if len(chapters) > 0 {
			s.database.BatchSaveChapters(chapters[0].BookNumber, chapters)
		}
	}
}

// Close 关闭异步保存器
func (s *AsyncDBSaver) Close() {
	close(s.saveChan)
	s.wg.Wait()
}