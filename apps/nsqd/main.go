package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/judwhite/go-svc"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqd"
)

type program struct {
	once sync.Once
	nsqd *nsqd.NSQD
}

//a. 通过第三方 svc 包进行优雅的后台进程管理，svc.Run() -> svc.Init() -> svc.Start()，启动 nsqd 实例；
/**
nsqd的main函数在apps/nsqd/main.go文件。
启动时调用了一个第三方包svc，主要作用是拦截syscall.SIGINT/syscall.SIGTERM这两个信号，最终还是调用了main.go下的3个方法：

program.Init()：windows下特殊操作
program.Start()：加载参数和配置文件、加载上一次保存的Topic信息并完成初始化、创建nsqd并调用p.nsqd.Main()启动
program.Stop()：退出处理

p.nsqd.Main()的逻辑也很简单，代码不贴了，依次启动了TCP服务、HTTP服务、HTTPS服务这3个服务。除此之外，还启动了以下两个协程：

queueScanLoop：消息延时/超时处理
lookupLoop：服务注册
*/
func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%s", err)
	}
}

func (p *program) Init(env svc.Environment) error {
	opts := nsqd.NewOptions()

	flagSet := nsqdFlagSet(opts)
	flagSet.Parse(os.Args[1:])

	rand.Seed(time.Now().UTC().UnixNano())

	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqd"))
		os.Exit(0)
	}

	var cfg config
	configFile := flagSet.Lookup("config").Value.String()
	if configFile != "" {
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			logFatal("failed to load config file %s - %s", configFile, err)
		}
	}
	cfg.Validate()

	options.Resolve(opts, flagSet, cfg)
	//b. 初始化配置项（ opts, cfg ），加载历史数据（ nsqd.LoadMetadata ）、持久化最新数据（ nsqd.PersistMetadata ），然后开启协程，进入 nsqd.Main() 主函数；
	nsqd, err := nsqd.New(opts)
	if err != nil {
		logFatal("failed to instantiate nsqd - %s", err)
	}
	p.nsqd = nsqd

	return nil
}

func (p *program) Start() error {
	err := p.nsqd.LoadMetadata()
	if err != nil {
		logFatal("failed to load metadata - %s", err)
	}
	err = p.nsqd.PersistMetadata()
	if err != nil {
		logFatal("failed to persist metadata - %s", err)
	}

	go func() {
		err := p.nsqd.Main()
		if err != nil {
			p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}

func (p *program) Stop() error {
	p.once.Do(func() {
		p.nsqd.Exit()
	})
	return nil
}

func (p *program) Handle(s os.Signal) error {
	return svc.ErrStop
}

// Context returns a context that will be canceled when nsqd initiates the shutdown
func (p *program) Context() context.Context {
	return p.nsqd.Context()
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqd] ", f, args...)
}
