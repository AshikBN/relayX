package app

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/micvbang/go-helpy/sizey"
	"github.com/spf13/cobra"
	"golang.org/x/net/netutil"
)

type ServeFlags struct {
	logLevel int

	s3BucketName string

	httpListenAddress  string
	httpListenPort     int
	httpConnectionsMax int
	httpAPIKey         string

	httpEnableDebug        bool
	httpDebugListenAddress string
	httpDebugListenPort    int

	cacheDir              string
	cacheMaxBytes         int64
	cacheEvictionInterval time.Duration

	recordBatchBlockTime    time.Duration
	recordBatchSoftMaxBytes int
	recordBatchMaxRecords   int
	recordBatchHardMaxBytes int
}

var serveFlags ServeFlags

func init() {
	fs := serveCmd.Flags()

	//logger code pending
	//fs.IntVar(&ServeFlags.logLevel,"log-level",int(logger.LevelInfo),"Log level, info=4, debug=5")

	//http
	fs.StringVar(&serveFlags.httpListenAddress, "http-address", "127.0.0.1", "address to listen to HTTP traffic")
	fs.IntVar(&serveFlags.httpListenPort, "http-port", 51313, "Port to listen for HTTP traffic")
	fs.StringVar(&serveFlags.httpAPIKey, "http-api-key", "api-key", "API key for authorizing HTTP requests") // this is not safe and needs to be changed
	fs.IntVar(&serveFlags.httpConnectionsMax, "http-connections", runtime.NumCPU()*64, "Maximum number of concurrent incoming HTTP connections to be handled")

	//// http debug
	fs.BoolVar(&serveFlags.httpEnableDebug, "http-debug-enable", false, "Whether to enable DEBUG endpoints")
	fs.StringVar(&serveFlags.httpDebugListenAddress, "http-debug-address", "127.0.0.1", "Address to expose DEBUG endpoints. You very likely want this to remain localhost!")
	fs.IntVar(&serveFlags.httpDebugListenPort, "http-debug-port", 5000, "Port to serve DEBUG endpoints on")

	// s3
	fs.StringVar(&serveFlags.s3BucketName, "s3-bucket", "", "Bucket name")

	//caching
	fs.StringVar(&serveFlags.cacheDir, "cache-dir", path.Join(os.TempDir(), "relayx-cache"), "local directory to use when caching record batches")
	fs.Int64Var(&serveFlags.cacheMaxBytes, "cache-maxsize", 1*sizey.GB, "maximum number of bytes to keep in cache")
	fs.DurationVar(&serveFlags.cacheEvictionInterval, "cache-eviction-internal", 5*time.Minute, "amount of time between enforching maximum cache size")

	// batching
	fs.DurationVar(&serveFlags.recordBatchBlockTime, "batch-wait-time", time.Second, "Amount of time to wait between receiving first record in batch and committing the batch")
	fs.IntVar(&serveFlags.recordBatchSoftMaxBytes, "batch-bytes-soft-max", 10*sizey.MB, "Soft maximum for the size of a batch")
	fs.IntVar(&serveFlags.recordBatchHardMaxBytes, "batch-bytes-hard-max", 30*sizey.MB, "Hard maximum for the size of a batch")
	fs.IntVar(&serveFlags.recordBatchMaxRecords, "batch-records-hard-max", 32*1024, "Hard maximum for the number of records a batch can contain")

	//required flags
	serveCmd.MarkFlagRequired("s3-bucket")

}

var serveCmd = &cobra.Command{
	Use:   "http-server",
	Short: "start HTTP server",
	Long:  "start relayX's http server",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		flags := serveFlags

		//logging pending

		//cache creation pending

		//makeblocking s3 pending

		//batch pool pending

		mux := http.NewServeMux()

		//register route pending

		errs := make(chan error, 8)

		go func() {
			addr := fmt.Sprintf("%s:%d", flags.httpDebugListenAddress, flags.httpListenPort)
			//log infor pendding

			listener, err := net.Listen("tcp", addr)
			if err != nil {
				errs <- fmt.Errorf("listening on %s: %w", addr, err)
			}
			defer listener.Close()

			listener = netutil.LimitListener(listener, flags.httpConnectionsMax)
			errs <- http.Serve(listener, mux)

		}()

		//http debug handler pending

		//error handling pending
	},
}
