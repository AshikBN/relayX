package relayxcache

import (
	"context"
	"fmt"
	"time"

	"github.com/AshikBN/relayX/internal/infrastructure/logger"
)

func EvictionLoop(ctx context.Context, log logger.Logger, cache *Cache, cacheMaxBytes int64, interval time.Duration) error {
	log = log.
		WithField("max bytes", cacheMaxBytes).
		WithField("interval", interval)

	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:

		}

		cacheSize := cache.size()
		if cacheSize < cacheMaxBytes {
			continue
		}

		fillLevel := (float32(cacheSize) / float32(cacheMaxBytes)) * 100
		log.Infof("cache is full(%.2f%% %d/%d),evicting items", fillLevel, cacheSize, cacheMaxBytes)
		err := cache.EvictLeastReacentlyUsed(cacheMaxBytes)
		if err != nil {
			return fmt.Errorf("eviction cache:%w", err)
		}
	}
}
