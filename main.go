package main

import (
	"fmt"
	"os"
	"super-redis/config"
	"super-redis/lib/logger"
	RedisServerImpl "super-redis/redis/server"
	"super-redis/tcp"
	"super-redis/utils"
)

func main() {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "super-redis",
		Ext:        ".log",
		TimeFormat: "2021-06-28",
	})
	configFilename := os.Getenv("CONFIG")
	if configFilename == "" {
		if utils.FileExists("redis.conf") {
			config.SetupConfig("redis.conf")
		} else {
			config.Properties = config.DefaultProperties
		}
	} else {
		config.SetupConfig(configFilename)
	}

	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, RedisServerImpl.MakeHandler())
	if err != nil {
		logger.Error(err)
	}
}
