package relayxcache

import (
	"io"
	"sync"
	"time"

	"github.com/AshikBN/relayX/internal/infrastructure/logger"
	"github.com/AshikBN/relayX/internal/infrastructure/nops"
	"github.com/AshikBN/relayX/relayxerr"
	"github.com/micvbang/go-helpy/bytey"
)

type memoryCacheItem struct {
	buf        *bytey.Buffer
	AccessedAt time.Time
}

type MemoryCache struct {
	log logger.Logger
	now func() time.Time

	mu    sync.Mutex
	items map[string]memoryCacheItem
}

func NewMemoryStorage(log logger.Logger) *MemoryCache {
	return &MemoryCache{
		log:   log,
		now:   time.Now,
		items: make(map[string]memoryCacheItem, 64),
	}
}

func (mc *MemoryCache) Reader(key string) (io.ReadSeekCloser, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	item, ok := mc.items[key]
	if !ok {
		return nil, relayxerr.ErrNotInCache
	}

	item.AccessedAt = mc.now()
	mc.items[key] = item

	buf := bytey.NewBuffer(item.buf.Bytes())
	return nops.NopReadSeekCloser(buf), nil

}

func (mc *MemoryCache) Writer(key string) (io.WriteCloser, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	buf := bytey.NewBuffer(make([]byte, 0, 10))
	memoryCacheItem := memoryCacheItem{
		buf:        buf,
		AccessedAt: mc.now(),
	}
	mc.items[key] = memoryCacheItem
	return nops.NopWriteCloser(buf), nil

}

func (mc *MemoryCache) Remove(key string) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	delete(mc.items, key)
	return nil
}

func (mc *MemoryCache) List() (map[string]CacheItem, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	cacheItems := make(map[string]CacheItem, len(mc.items))
	for key, item := range mc.items {
		cacheItems[key] = CacheItem{
			Size:       int64(item.buf.Len()),
			AccessedAt: item.AccessedAt,
			Key:        key,
		}
	}
	return cacheItems, nil
}

func (mc *MemoryCache) SizeOf(key string) (CacheItem, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	cacheItem, ok := mc.items[key]
	if !ok {
		return CacheItem{}, relayxerr.ErrNotInCache
	}
	return CacheItem{
		Size:       int64(cacheItem.buf.Len()),
		AccessedAt: cacheItem.AccessedAt,
		Key:        key,
	}, nil
}
