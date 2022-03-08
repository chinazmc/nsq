package protocol

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"

	"github.com/nsqio/nsq/internal/lg"
)

type TCPHandler interface {
	Handle(net.Conn)
}

func TCPServer(listener net.Listener, handler TCPHandler, logf lg.AppLogFunc) error {
	logf(lg.INFO, "TCP: listening on %s", listener.Addr())

	var wg sync.WaitGroup

	for {
		// 等待生产者或消费者连接
		clientConn, err := listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				logf(lg.WARN, "temporary Accept() failure - %s", err)
				runtime.Gosched()
				continue
			}
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("listener.Accept() error - %s", err)
			}
			break
		}
		// 每创建一个连接wg +1
		wg.Add(1)
		go func() {
			// 每个连接均启动一个独立的协程来接收处理请求
			handler.Handle(clientConn)
			wg.Done()
		}()
	}

	// wait to return until all handler goroutines complete
	wg.Wait()

	logf(lg.INFO, "TCP: closing %s", listener.Addr())

	return nil
}
