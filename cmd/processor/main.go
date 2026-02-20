package main

import (
	"github.com/yaninyzwitty/idempotent-widthrawal-processor/internal/app/di"
)

func main() {
	app := di.NewFxApp()

	app.Run()
}
