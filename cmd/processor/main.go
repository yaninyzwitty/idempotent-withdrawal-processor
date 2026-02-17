package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/app/di"
	"go.uber.org/fx"
)

func main() {
	app := di.NewFxApp(
		fx.Invoke(setupSignalHandler),
	)

	app.Run()
}

func setupSignalHandler(lifecycle fx.Lifecycle, shutdowner fx.Shutdowner) {
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				sigCh := make(chan os.Signal, 1)
				signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
				<-sigCh
				_ = shutdowner.Shutdown()
			}()
			return nil
		},
	})
}
