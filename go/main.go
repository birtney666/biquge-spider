package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	hostname, _ := os.Hostname()
	nodeID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	config, err := LoadConfig("")
	if err != nil {
		fmt.Printf("加载配置文件失败: %v\n", err)
		return
	}
	config = config.ToTimeConfig()
	
	node, err := NewCrawlerNode(nodeID, config)
	if err != nil {
		fmt.Printf("创建爬虫节点失败: %v\n", err)
		return
	}
	defer node.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool)
	go func() {
		node.Run()
		done <- true
	}()

	select {
	case <-sigChan:
		fmt.Printf("\n[%s] 🛑 接收到中断信号，开始优雅关闭...\n", nodeID)
		node.SetShutdown()

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
