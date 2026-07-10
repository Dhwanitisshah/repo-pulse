package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/redis/go-redis/v9"

	ghclient "repo-pulse/ingest/internal/github"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := loadConfig()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	slog.Info("ingest starting",
		"redis_addr", cfg.RedisAddr,
		"watched_repos", cfg.WatchedRepos,
		"poll_interval", cfg.PollInterval.String(),
		"stream", streamName,
	)

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ghc := ghclient.NewClient(cfg.GitHubToken)

	var wg sync.WaitGroup
	for _, repo := range cfg.WatchedRepos {
		poller := newRepoPoller(ghc, rdb, repo, cfg.PollInterval)
		wg.Add(1)
		go func() {
			defer wg.Done()
			poller.run(ctx)
		}()
	}

	wg.Wait()
	slog.Info("shutdown complete, redis client closed")
}
