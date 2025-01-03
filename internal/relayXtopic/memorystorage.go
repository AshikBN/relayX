package relayxtopic

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/AshikBN/relayX/internal/infrastructure/logger"
	"github.com/AshikBN/relayX/internal/infrastructure/nops"
	"github.com/AshikBN/relayX/relayxerr"
)

type MemoryTopicStorage struct {
	mu      sync.Mutex
	storage map[string]*bytes.Buffer
}

func NewMemoryStorage(log logger.Logger) *MemoryTopicStorage {
	return &MemoryTopicStorage{
		storage: make(map[string]*bytes.Buffer, 64),
	}
}

func (ms *MemoryTopicStorage) Writer(key string) (io.WriteCloser, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	buf := bytes.NewBuffer(make([]byte, 0, 4096))
	ms.storage[key] = buf

	return nops.NopWriteCloser(buf), nil
}

func (ms *MemoryTopicStorage) Reader(key string) (io.ReadCloser, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	buf, ok := ms.storage[key]
	if !ok {
		return nil, relayxerr.ErrNotInStorage
	}

	return io.NopCloser(buf), nil
}

func (ms *MemoryTopicStorage) ListFiles(topicName string, extension string) ([]File, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	files := make([]File, 0, 128)
	topicPrefix := fmt.Sprintf("%s/", topicName)
	for key, buf := range ms.storage {
		if strings.HasPrefix(key, topicPrefix) {
			files = append(files, File{
				Size: int64(buf.Len()),
				Path: key,
			})
		}
	}

	return files, nil
}
