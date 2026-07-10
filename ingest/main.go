package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

const streamName = "events"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer rdb.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("ingest starting", "redis_addr", redisAddr, "stream", streamName)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	var seq int64

	for {
		select {
		case <-ctx.Done():
			slog.Info("shutdown signal received, closing redis client")
			return
		case <-ticker.C:
			seq++
			ts := time.Now().UnixMilli()

			id, err := rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: streamName,
				Values: map[string]interface{}{
					"type": "heartbeat",
					"ts":   ts,
					"seq":  seq,
				},
			}).Result()

			if err != nil {
				slog.Error("failed to publish heartbeat", "error", err, "seq", seq)
				continue
			}

			slog.Info("heartbeat published", "id", id, "seq", seq, "ts", ts)
		}
	}
}
