package relayxcache

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/micvbang/go-helpy/filepathy"
)

type CacheItem struct {
	Size       int64
	AccessedAt time.Time
	Key        string
}

type DiskCache struct {
	log     logger.Logger
	rootDir string
	tempDir string
}

type cacheWriter struct {
	tempFile *os.File
	destPath string
}

// creat the new disk storage
func NewDiskStorage(log logger.Logger, rootDir string) (*DiskCache, error) {
	if !strings.HasSuffix(rootDir, "/") {
		rootDir += "/"
	}
	tempDir := filepath.Join(rootDir, "_tmpdir")
	//create the tempdir and all the its parent directories and also sets the full access
	err := os.MkdirAll(tempDir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("creating tempdir %s:%w", tempDir, err)
	}

	return &DiskCache{
		log:     log,
		rootDir: rootDir,
		tempDir: tempDir,
	}, nil

}

// get the list of all the cached files in the disk cache storage
func (c *DiskCache) List() (map[string]CacheItem, error) {
	cachedItems := make(map[string]CacheItem, 64)

	fileWalkConfig := filepathy.WalkConfig{
		Files:     true,
		Recursive: true,
	}
	//finding and creating the map of all the cached items in the root directory
	err := filepathy.Walk(c.rootDir, fileWalkConfig, func(path string, info os.FileInfo, err error) error {
		trimmedPath := strings.TrimPrefix(path, c.rootDir)
		cachedItems[trimmedPath] = CacheItem{
			Size:       info.Size(),
			AccessedAt: info.ModTime(),
			Key:        trimmedPath,
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reading existing file:%w", err)
	}
	return cachedItems, nil

}

func (c *DiskCache) Writer(key string) (io.WriteCloser, error) {

	//logging pending
	destPath, err := c.cachePath(key)
	if err != nil {
		return nil, fmt.Errorf("getting cache path %s:%s", key, err)
	}
	return NewCacheWriter(c.tempDir, destPath)
}

func (c *DiskCache) Remove(key string) error {
	path, err := c.cachePath(key)
	if err != nil {
		return fmt.Errorf("getting the cache path %s:%w", key, err)
	}
	return os.Remove(path)
}

func (c *DiskCache) Reader(key string) (io.ReadSeekCloser, error) {

	//create log pending
	path, err := c.cachePath(key)
	if err != nil {
		return nil, fmt.Errorf("getting the cache file %s:%w", key, err)
	}
	f, err := os.Open(path)
	if err != nil {
		//logging and returning pending
		return nil, fmt.Errorf("")
	}
	//long pending
	return f, nil

}

func (c *DiskCache) SizeOf(key string) (CacheItem, error) {
	//log pending
	fileInfo, err := os.Stat(key)
	if err != nil {
		return CacheItem{}, fmt.Errorf("getting the stat of %s", key)
	}
	//log pending
	return CacheItem{
		Size:       fileInfo.Size(),
		AccessedAt: fileInfo.ModTime(),
	}, nil
}

// returting path of the cache file
func (c *DiskCache) cachePath(key string) (string, error) {
	abs, err := filepath.Abs(filepath.Join(c.rootDir, key))
	if err != nil {
		return "", fmt.Errorf("getting the abs path of %s:%w", key, err)
	}
	if !strings.HasPrefix(abs, c.rootDir) {
		err := fmt.Errorf("attempting to access the path outside of the rootdir: %s:%w", c.rootDir) //seberror pending
		//logging pending
		return "", err
	}
	return filepath.Join(c.rootDir, key), nil

}

// cache writer
func NewCacheWriter(tempDir string, destpath string) (*cacheWriter, error) {
	tmpFile, err := os.CreateTemp(tempDir, "relay_*")
	if err != nil {
		return nil, fmt.Errorf("creating the temp file:%w", err)
	}
	return &cacheWriter{
		tempFile: tmpFile,
		destPath: destpath,
	}, nil
}

func (cw *cacheWriter) Write(bs []byte) (int, error) {
	return cw.tempFile.Write(bs)
}

func (cw *cacheWriter) Close() error {
	err := cw.tempFile.Close()
	if err != nil {
		return fmt.Errorf("closing the cacheWriter file %w", err)
	}
	cacheDir := path.Dir(cw.destPath)
	err = os.MkdirAll(cacheDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("creating the cache dir %s:%w", cacheDir, err)
	}
	err = os.Rename(cw.tempFile.Name(), cw.destPath)
	if err != nil {
		fmt.Errorf("moving %s to %s:%w", cw.tempFile.Name(), cw.destPath, err)
	}
	return nil

}
