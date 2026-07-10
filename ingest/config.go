package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type config struct {
	RedisAddr    string
	GitHubToken  string
	WatchedRepos []string
	PollInterval time.Duration
}

func loadConfig() (config, error) {
	cfg := config{
		RedisAddr:    getEnvDefault("REDIS_ADDR", "localhost:6379"),
		GitHubToken:  os.Getenv("GITHUB_TOKEN"),
		PollInterval: 15 * time.Second,
	}

	if cfg.GitHubToken == "" {
		return cfg, fmt.Errorf("GITHUB_TOKEN is required but empty")
	}

	for _, r := range strings.Split(os.Getenv("WATCHED_REPOS"), ",") {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		if !strings.Contains(r, "/") {
			return cfg, fmt.Errorf("invalid entry in WATCHED_REPOS: %q (want owner/name)", r)
		}
		cfg.WatchedRepos = append(cfg.WatchedRepos, r)
	}
	if len(cfg.WatchedRepos) == 0 {
		return cfg, fmt.Errorf("WATCHED_REPOS is required but empty (comma-separated owner/name list)")
	}

	if s := os.Getenv("POLL_INTERVAL_SECONDS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			return cfg, fmt.Errorf("invalid POLL_INTERVAL_SECONDS: %q", s)
		}
		cfg.PollInterval = time.Duration(n) * time.Second
	}

	return cfg, nil
}

func getEnvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
