package relayxcache

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/AshikBN/relayX/internal/infrastructure/logger"
)

type Storage interface {
	Reader(key string) (io.ReadSeekCloser, error)
	Writer(key string) (io.WriteCloser, error)
	Remove(key string) error
	List() (map[string]CacheItem, error)
	SizeOf(key string) (CacheItem, error)
}

type Cache struct {
	log     logger.Logger
	storage Storage
	now     func() time.Time

	mu         sync.Mutex
	cacheItems map[string]CacheItem
}

func NewDiskCache(log logger.Logger, rootDir string) (*Cache, error) {
	diskStorage, err := NewDiskStorage(log, rootDir)
	if err != nil {
		return nil, fmt.Errorf("creating the new disk storage:%w", err)
	}
	return New(log, diskStorage)
}

func New(log logger.Logger, cacheStorage Storage) (*Cache, error) {
	return NewCacheWithNow(log, cacheStorage, time.Now)
}

func NewCacheWithNow(log logger.Logger, cacheStorage Storage, now func() time.Time) (*Cache, error) {
	cacheItems, err := cacheStorage.List()
	if err != nil {
		return nil, fmt.Errorf("listing existing files:%w", err)
	}
	return &Cache{
		log:        log,
		storage:    cacheStorage,
		now:        now,
		cacheItems: cacheItems,
	}, nil

}

func (c *Cache) Writer(key string) (io.WriteCloser, error) {
	log := c.log.WithField("key", key)

	w, err := c.storage.Writer(key)
	if err != nil {
		return nil, err
	}

	return newWriteCloseWrapper(w, func(size int64) {
		log.Debugf("adding to cache items")
		c.mu.Lock()
		defer c.mu.Unlock()
		c.cacheItems[key] = CacheItem{
			Size:       size,
			AccessedAt: c.now(),
			Key:        key,
		}
	}), nil
}

func (c *Cache) Write(key string, bs []byte) (int, error) {
	wtr, err := c.Writer(key)
	if err != nil {
		return 0, fmt.Errorf("creating the writer %w", err)
	}
	defer wtr.Close()
	return wtr.Write(bs)
}

func (c *Cache) Reader(key string) (io.ReadSeekCloser, error) {
	log := c.log.WithField("key", key)
	r, err := c.storage.Reader(key)
	if err != nil {
		return nil, fmt.Errorf("reading the cache file %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.cacheItems[key]
	if !ok {
		log.Debugf("not found cache items,adding")
		newItem, err := c.storage.SizeOf(key)
		if err == nil {
			item = newItem
		}

	}
	item.AccessedAt = c.now()
	// item.Key=key
	c.cacheItems[key] = item
	return r, nil

}

func (c *Cache) Size() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.size()
}

func (c *Cache) size() int64 {
	c.log.Debugf("computing the size of %d items", len(c.cacheItems))
	size := int64(0)
	for _, item := range c.cacheItems {
		size += item.Size
	}
	return size
}

func (c *Cache) EvictLeastReacentlyUsed(maxSize int64) error {
	log := c.log.WithField("maxSize", maxSize)

	c.mu.Lock()
	defer c.mu.Unlock()

	cacheItems := make([]CacheItem, 0, len(c.cacheItems))
	for _, cacheItem := range c.cacheItems {
		cacheItems = append(cacheItems, cacheItem)
	}
	sort.Slice(cacheItems, func(i int, j int) bool {
		return cacheItems[j].AccessedAt.Before(cacheItems[i].AccessedAt)
	})
	currSize := int64(0)
	itemsDeleted := 0
	bytesDeleted := int64(0)
	for _, cacheItem := range cacheItems {
		if currSize > maxSize {
			err := c.storage.Remove(cacheItem.Key)
			if err != nil {
				log.Errorf("deleting %s:%w", cacheItem.Key, err)
				return fmt.Errorf("deleting %s:%w", cacheItem.Key, err)
			}
			itemsDeleted += 1
			bytesDeleted += cacheItem.Size
			delete(c.cacheItems, cacheItem.Key)
		} else {
			currSize += cacheItem.Size
		}

	}
	cacheSize := c.size()
	log.Infof("deleted %d items(%d bytes)->cashe is now %d bytes", itemsDeleted, bytesDeleted, cacheSize)
	return nil

}

type writeCloseWrapper struct {
	wc         io.WriteCloser
	size       int64
	afterClose func(int64)
}

func newWriteCloseWrapper(wc io.WriteCloser, afterClose func(int64)) *writeCloseWrapper {
	return &writeCloseWrapper{
		wc:         wc,
		afterClose: afterClose,
	}
}

func (w *writeCloseWrapper) Write(bs []byte) (int, error) {
	n, err := w.wc.Write(bs)
	w.size += int64(n)
	return n, err
}

func (w *writeCloseWrapper) Close() error {
	err := w.wc.Close()
	if err != nil {
		return fmt.Errorf("closing the writeclose wrapper file:%w", err)
	}
	w.afterClose(w.size)
	return nil
}
