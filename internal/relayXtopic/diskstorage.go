package relayxtopic

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/AshikBN/relayX/internal/infrastructure/logger"
	"github.com/AshikBN/relayX/relayxerr"
)

type DiskStorage struct {
	log     logger.Logger
	rootDir string
}

func NewDiskStorage(logger logger.Logger, rootDir string) *DiskStorage {
	return &DiskStorage{
		log:     logger,
		rootDir: rootDir,
	}
}

func (ds *DiskStorage) Writer(key string) (io.WriteCloser, error) {
	batchPath := ds.rootDirPath(key)

	log := ds.log.WithField("key", key).WithField("path", batchPath)
	log.Debugf("creating dirs")

	err := os.MkdirAll(filepath.Dir(batchPath), os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("creating topic dir:%w", err)
	}
	log.Debugf("creating files")
	f, err := os.Create(batchPath)
	if err != nil {
		return nil, fmt.Errorf("opening the file %s:%w", batchPath, err)
	}

	return f, nil

}

func (ds *DiskStorage) Reader(key string) (io.ReadCloser, error) {
	batchPath := ds.rootDirPath(key)

	log := ds.log.WithField("key", key).WithField("path", batchPath)
	log.Debugf("opening file")

	f, err := os.Open(batchPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = errors.Join(err, relayxerr.ErrNotInStorage)
		}
		return nil, fmt.Errorf("reading record batch %s;%w", batchPath, err)
	}

	return f, nil
}

func (ds *DiskStorage) ListFiles(topicName string, extension string) ([]File, error) {
	log := ds.log.WithField("topicName", topicName).WithField("extension", extension)
	log.Debugf("listing files")

	t0 := time.Now()
	topicPath := ds.rootDirPath(topicName)

	files := make([]File, 0, 128)
	err := filepath.Walk(topicPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		if filepath.Ext(path) == "."+extension {
			files = append(files, File{
				Size: info.Size(),
				Path: path,
			})
		}
		return nil
	})

	if err != nil && os.IsNotExist(err) {
		return files, nil
	}

	log.Debugf("found %d files %s", len(files), time.Since(t0))
	return files, err

}

func (ds *DiskStorage) rootDirPath(key string) string {
	return filepath.Join(ds.rootDir, key)
}
