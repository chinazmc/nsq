package nsqd

import (
	"io"
	"net"
	"sync"

	"github.com/nsqio/nsq/internal/protocol"
)

const (
	typeConsumer = iota
	typeProducer
)

type Client interface {
	Type() int
	Stats(string) ClientStats
}

//nsqd/tcp.go文件，tcpServer通过Handle()方法接收TCP请求。
//tcpServer是nsqd结构的成员，全局也就只有一个实例，但在protocol包的TCPServer方法中，每创建一个新的连接，均会调用一次tcpServer.Handle()
type tcpServer struct {
	nsqd  *NSQD
	conns sync.Map
}

/*
	p.nsqd.Main()启动protocol.TCPServer()，这个方法里会为每个客户端连接创建一个新协程，协程执行tcpServer.Handle()方法
	本方法首先对新连接读取4字节校验版本，新连接必须首先发送4字节"  V2"。
	然后阻塞调用nsqd.protocolV2.IOLoop()处理客户端接下来的请求。

TCPServer的核心是为每个连接启动的协程处理方法handler.Handle(clientConn)，实际调用的是下面这个方法，连接建立时先读取4字节，
必须是" V2"，然后启动prot.IOLoop(clientConn)处理接下来的客户端请求：
*/
func (p *tcpServer) Handle(conn net.Conn) {
	p.nsqd.logf(LOG_INFO, "TCP: new client(%s)", conn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	// 无论是生产者还是消费者，建立连接时，必须先发送4字节的"  V2"进行版本校验
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		p.nsqd.logf(LOG_ERROR, "failed to read protocol version - %s", err)
		conn.Close()
		return
	}
	protocolMagic := string(buf)

	p.nsqd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'",
		conn.RemoteAddr(), protocolMagic)
	//e. TCP 解析 V2 协议，走内部协议封装的 prot.IOLoop(conn) 进行处理；
	var prot protocol.Protocol
	switch protocolMagic {
	case "  V2":
		prot = &protocolV2{nsqd: p.nsqd}
	default:
		protocol.SendFramedResponse(conn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		conn.Close()
		p.nsqd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
			conn.RemoteAddr(), protocolMagic)
		return
	}

	client := prot.NewClient(conn)
	// 版本校验通过，保存连接信息，key-是ADDR，value是当前连接指针
	p.conns.Store(conn.RemoteAddr(), client)
	// 启动
	err = prot.IOLoop(client)
	if err != nil {
		p.nsqd.logf(LOG_ERROR, "client(%s) - %s", conn.RemoteAddr(), err)
	}

	p.conns.Delete(conn.RemoteAddr())
	client.Close()
}

func (p *tcpServer) Close() {
	p.conns.Range(func(k, v interface{}) bool {
		v.(protocol.Client).Close()
		return true
	})
}
