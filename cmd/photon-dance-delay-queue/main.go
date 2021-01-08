package main

import (
	"context"
	"flag"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	conf "github.com/amazingchow/photon-dance-delay-queue/internal/config"
	"github.com/amazingchow/photon-dance-delay-queue/internal/util"
)

var (
	cfgPathFlag = flag.String("conf", "conf/delay_queue.json", "delay queue config file")
	verboseFlag = flag.Bool("verbose", false, "set verbose output")
)

func main() {
	flag.Parse()

	// 设置全局logger
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if *verboseFlag {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	// 加载节点配置
	var cfg conf.DelayQueue
	util.LoadConfigFileOrPanic(*cfgPathFlag, &cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
	}()

	srv := newTaskDelayQueueServiceServer(&cfg)
	defer func() {
		srv.close()
	}()

	// 开启grpc服务
	go serveGPRC(ctx, srv, cfg.GRPCEndpoint)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	for range sigCh {
		break
	}

	log.Info().Msg("stop task delay-queue service")
}