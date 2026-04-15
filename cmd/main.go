package main

import (
	"context"
	"log"

	"github.com/martketplace-vkr/payment/config"
	"github.com/martketplace-vkr/payment/internal/app"

	cfgloader "github.com/martketplace-vkr/pkg/config"
)

func main() {
	ctx := context.Background()
	cfg := &config.Config{}

	if err := cfgloader.LoadConfig(ctx, cfg); err != nil {
		log.Fatal(err.Error())
	}

	if err := app.Run(ctx, cfg); err != nil {
		log.Fatal(err.Error())
	}
}
