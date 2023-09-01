// Copyright 2016 Google LLC. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The trillian_log_server binary runs the Trillian log server, and also
// provides an admin server.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"fmt"
	_ "net/http/pprof" // Register pprof HTTP handlers.
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/trillian"
	"github.com/google/trillian/cmd"
	"github.com/google/trillian/extension"
	"github.com/google/trillian/monitoring"
	"github.com/google/trillian/monitoring/opencensus"
	"github.com/google/trillian/monitoring/prometheus"
	"github.com/google/trillian/quota"
	"github.com/google/trillian/quota/etcd"
	"github.com/google/trillian/quota/etcd/quotaapi"
	"github.com/google/trillian/quota/etcd/quotapb"
	"github.com/google/trillian/server"
	"github.com/google/trillian/storage"
	"github.com/google/trillian/trees"
	"github.com/google/trillian/util"
	"github.com/google/trillian/util/clock"
	"github.com/jmhodges/trillian-s3-mysql/cmd/internal/serverutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"

	// Register supported storage providers.
	_ "github.com/google/trillian/storage/mysql"

	// Load quota providers
	_ "github.com/google/trillian/quota/crdbqm"
	_ "github.com/google/trillian/quota/mysqlqm"
)

var (
	rpcEndpoint     = flag.String("rpc_endpoint", "localhost:8090", "Endpoint for RPC requests (host:port)")
	httpEndpoint    = flag.String("http_endpoint", "localhost:8091", "Endpoint for HTTP metrics (host:port, empty means disabled)")
	healthzTimeout  = flag.Duration("healthz_timeout", time.Second*5, "Timeout used during healthz checks")
	tlsCertFile     = flag.String("tls_cert_file", "", "Path to the TLS server certificate. If unset, the server will use unsecured connections.")
	tlsKeyFile      = flag.String("tls_key_file", "", "Path to the TLS server key. If unset, the server will use unsecured connections.")
	etcdService     = flag.String("etcd_service", "trillian-logserver", "Service name to announce ourselves under")
	etcdHTTPService = flag.String("etcd_http_service", "trillian-logserver-http", "Service name to announce our HTTP endpoint under")

	quotaSystem = flag.String("quota_system", "mysql", fmt.Sprintf("Quota system to use. One of: %v", quota.Providers()))
	quotaDryRun = flag.Bool("quota_dry_run", false, "If true no requests are blocked due to lack of tokens")

	storageSystem = flag.String("storage_system", "mysql", fmt.Sprintf("Storage system to use. One of: %v", storage.Providers()))

	treeGCEnabled            = flag.Bool("tree_gc", true, "If true, tree garbage collection (hard-deletion) is periodically performed")
	treeDeleteThreshold      = flag.Duration("tree_delete_threshold", serverutil.DefaultTreeDeleteThreshold, "Minimum period a tree has to remain deleted before being hard-deleted")
	treeDeleteMinRunInterval = flag.Duration("tree_delete_min_run_interval", serverutil.DefaultTreeDeleteMinInterval, "Minimum interval between tree garbage collection sweeps. Actual runs happen randomly between [minInterval,2*minInterval).")

	tracing          = flag.Bool("tracing", false, "If true opencensus Stackdriver tracing will be enabled. See https://opencensus.io/.")
	tracingProjectID = flag.String("tracing_project_id", "", "project ID to pass to stackdriver. Can be empty for GCP, consult docs for other platforms.")
	tracingPercent   = flag.Int("tracing_percent", 0, "Percent of requests to be traced. Zero is a special case to use the DefaultSampler")

	configFile = flag.String("config", "", "Config file containing flags, file contents can be overridden by command line flags")

	// Profiling related flags.
	cpuProfile = flag.String("cpuprofile", "", "If set, write CPU profile to this file")
	memProfile = flag.String("memprofile", "", "If set, write memory profile to this file")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	defer klog.Flush()

	if *configFile != "" {
		if err := cmd.ParseFlagFile(*configFile); err != nil {
			klog.Exitf("Failed to load flags from config file %q: %s", *configFile, err)
		}
	}
	klog.Info("**** Log Server Starting ****")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go util.AwaitSignal(ctx, cancel)

	var options []grpc.ServerOption
	mf := prometheus.MetricFactory{}
	monitoring.SetStartSpan(opencensus.StartSpan)

	if *tracing {
		opts, err := opencensus.EnableRPCServerTracing(*tracingProjectID, *tracingPercent)
		if err != nil {
			klog.Exitf("Failed to initialize stackdriver / opencensus tracing: %v", err)
		}
		// Enable the server request counter tracing etc.
		options = append(options, opts...)
	}

	sp, err := storage.NewProvider(*storageSystem, mf)
	if err != nil {
		klog.Exitf("Failed to get storage provider: %v", err)
	}
	defer func() {
		if err := sp.Close(); err != nil {
			klog.Errorf("Close(): %v", err)
		}
	}()

	var client *clientv3.Client
	if servers := *etcd.Servers; servers != "" {
		if client, err = clientv3.New(clientv3.Config{
			Endpoints:   strings.Split(servers, ","),
			DialTimeout: 5 * time.Second,
		}); err != nil {
			klog.Exitf("Failed to connect to etcd at %v: %v", servers, err)
		}
		defer func() {
			if err := client.Close(); err != nil {
				klog.Errorf("Close(): %v", err)
			}
		}()
	}

	// Announce our endpoints to etcd if so configured.
	unannounce := serverutil.AnnounceSelf(ctx, client, *etcdService, *rpcEndpoint, cancel)
	defer unannounce()

	if *httpEndpoint != "" {
		unannounceHTTP := serverutil.AnnounceSelf(ctx, client, *etcdHTTPService, *httpEndpoint, cancel)
		defer unannounceHTTP()
	}

	qm, err := quota.NewManager(*quotaSystem)
	if err != nil {
		klog.Exitf("Error creating quota manager: %v", err)
	}

	registry := extension.Registry{
		AdminStorage:  sp.AdminStorage(),
		LogStorage:    sp.LogStorage(),
		QuotaManager:  qm,
		MetricFactory: mf,
	}

	// Enable CPU profile if requested.
	if *cpuProfile != "" {
		f := mustCreate(*cpuProfile)
		if err := pprof.StartCPUProfile(f); err != nil {
			klog.Exitf("StartCPUProfile(): %v", err)
		}
		defer pprof.StopCPUProfile()
	}

	m := serverutil.Main{
		RPCEndpoint:  *rpcEndpoint,
		HTTPEndpoint: *httpEndpoint,
		TLSCertFile:  *tlsCertFile,
		TLSKeyFile:   *tlsKeyFile,
		StatsPrefix:  "log",
		ExtraOptions: options,
		QuotaDryRun:  *quotaDryRun,
		DBClose:      sp.Close,
		Registry:     registry,
		RegisterServerFn: func(s *grpc.Server, registry extension.Registry) error {
			logServer := server.NewTrillianLogRPCServer(registry, clock.System)
			if err := logServer.IsHealthy(); err != nil {
				return err
			}
			tiledLeavesByRangeServer := &tiledLeavesByRangeServer{
				registry: registry, logServer: logServer, tileSize: 1000, s3Prefix: "FIXME", s3Bucket: "FIXME", s3Service: nil} // FIXME s3 set up
			trillian.RegisterTrillianLogServer(s, tiledLeavesByRangeServer)
			if *quotaSystem == etcd.QuotaManagerName {
				quotapb.RegisterQuotaServer(s, quotaapi.NewServer(client))
			}
			return nil
		},
		IsHealthy: func(ctx context.Context) error {
			as := sp.AdminStorage()
			return as.CheckDatabaseAccessible(ctx)
		},
		HealthyDeadline:       *healthzTimeout,
		AllowedTreeTypes:      []trillian.TreeType{trillian.TreeType_LOG, trillian.TreeType_PREORDERED_LOG},
		TreeGCEnabled:         *treeGCEnabled,
		TreeDeleteThreshold:   *treeDeleteThreshold,
		TreeDeleteMinInterval: *treeDeleteMinRunInterval,
	}

	if err := m.Run(ctx); err != nil {
		klog.Exitf("Server exited with error: %v", err)
	}

	if *memProfile != "" {
		f := mustCreate(*memProfile)
		if err := pprof.WriteHeapProfile(f); err != nil {
			klog.Exitf("WriteHeapProfile(): %v", err)
		}
	}
}

func mustCreate(fileName string) *os.File {
	f, err := os.Create(fileName)
	if err != nil {
		klog.Fatal(err)
	}
	return f
}

var (
	optsLogRead = trees.NewGetOpts(trees.Query, trillian.TreeType_LOG, trillian.TreeType_PREORDERED_LOG)
)

type tiledLeavesByRangeServer struct {
	registry  extension.Registry
	logServer trillian.TrillianLogServer
	tileSize  int64

	s3Prefix  string
	s3Bucket  string
	s3Service *s3.Client
}

// AddSequencedLeaves implements trillian.TrillianLogServer.
func (s *tiledLeavesByRangeServer) AddSequencedLeaves(ctx context.Context, req *trillian.AddSequencedLeavesRequest) (*trillian.AddSequencedLeavesResponse, error) {
	return s.logServer.AddSequencedLeaves(ctx, req)
}

// GetConsistencyProof implements trillian.TrillianLogServer.
func (s *tiledLeavesByRangeServer) GetConsistencyProof(ctx context.Context, req *trillian.GetConsistencyProofRequest) (*trillian.GetConsistencyProofResponse, error) {
	return s.logServer.GetConsistencyProof(ctx, req)
}

// GetEntryAndProof implements trillian.TrillianLogServer.
func (s *tiledLeavesByRangeServer) GetEntryAndProof(ctx context.Context, req *trillian.GetEntryAndProofRequest) (*trillian.GetEntryAndProofResponse, error) {
	return s.logServer.GetEntryAndProof(ctx, req)
}

// GetInclusionProof implements trillian.TrillianLogServer.
func (s *tiledLeavesByRangeServer) GetInclusionProof(ctx context.Context, req *trillian.GetInclusionProofRequest) (*trillian.GetInclusionProofResponse, error) {
	return s.logServer.GetInclusionProof(ctx, req)
}

// GetInclusionProofByHash implements trillian.TrillianLogServer.
func (s *tiledLeavesByRangeServer) GetInclusionProofByHash(ctx context.Context, req *trillian.GetInclusionProofByHashRequest) (*trillian.GetInclusionProofByHashResponse, error) {
	return s.logServer.GetInclusionProofByHash(ctx, req)
}

// GetLatestSignedLogRoot implements trillian.TrillianLogServer.
func (s *tiledLeavesByRangeServer) GetLatestSignedLogRoot(ctx context.Context, req *trillian.GetLatestSignedLogRootRequest) (*trillian.GetLatestSignedLogRootResponse, error) {
	return s.logServer.GetLatestSignedLogRoot(ctx, req)
}

// GetLeavesByRange implements trillian.TrillianLogServer and does real things. FIXME
func (s *tiledLeavesByRangeServer) GetLeavesByRange(ctx context.Context, req *trillian.GetLeavesByRangeRequest) (*trillian.GetLeavesByRangeResponse, error) {
	// FIXME
	// err := validateGetLeavesByRangeRequest(req)
	// if err != nil {
	// 	return err
	// }

	tree, ctx, err := s.getTreeAndContext(ctx, req.LogId, optsLogRead)
	if err != nil {
		return nil, err
	}

	tile := s.makeTiledRequest(tree, req)

	// FIXME metadata.SendHeaders probably isn't what we want since it can be
	// only called one. Are we sure we need what the source was? Shouldn't we be
	// testing that another way?
	resp, _, err := s.getAndCacheTile(ctx, tile)
	if err != nil {
		return nil, err
	}

	// Truncate to match the request
	prefixToRemove := req.StartIndex - tile.req.StartIndex
	if prefixToRemove >= int64(len(resp.Leaves)) {
		// FIXME I believe this would already happen when the original
		// GetLeavesByRange is called?

		// In this case, the requested range is entirely outside the current log,
		// but the _tile_'s beginning was inside the log. For instance, a log with
		// size 1000 and max_getentries of 256, where ctile is handling a request
		// for start=1001&end=1001; the tile starts at offset 768, but is partial so
		// it doesn't include the requested range.
		//
		// When Trillian gets a request that is past the end of the log, it returns
		// 400 (for better or worse), so we emulate that here.
		return nil, status.Errorf(codes.OutOfRange, "requested range is outside the log")
	} else {
		resp.Leaves = resp.Leaves[prefixToRemove:]
	}

	// FIXME use grpc status codes or some such. The grpc status lib doesn't
	// support errors.Is and errors.As, however. See
	// https://github.com/grpc/grpc-go/issues/2934
	return resp, err
}

// InitLog implements trillian.TrillianLogServer.
func (s *tiledLeavesByRangeServer) InitLog(ctx context.Context, req *trillian.InitLogRequest) (*trillian.InitLogResponse, error) {
	return s.logServer.InitLog(ctx, req)
}

// QueueLeaf implements trillian.TrillianLogServer.
func (s *tiledLeavesByRangeServer) QueueLeaf(ctx context.Context, req *trillian.QueueLeafRequest) (*trillian.QueueLeafResponse, error) {
	return s.logServer.QueueLeaf(ctx, req)
}

func (s *tiledLeavesByRangeServer) getTreeAndContext(ctx context.Context, treeID int64, opts trees.GetOpts) (*trillian.Tree, context.Context, error) {
	tree, err := trees.GetTree(ctx, s.registry.AdminStorage, treeID, opts)
	if err != nil {
		return nil, nil, err
	}
	return tree, trees.NewContext(ctx, tree), nil
}

type tileRequest struct {
	req    *trillian.GetLeavesByRangeRequest
	treeID int64
}

// key returns the S3 key for the tile.
func (t tileRequest) key() string {
	return fmt.Sprintf("tree_id=%d/tile_size=%d/%d.cbor.gz", t.treeID, t.req.Count, t.req.StartIndex)
}

func (s *tiledLeavesByRangeServer) makeTiledRequest(tree *trillian.Tree, req *trillian.GetLeavesByRangeRequest) tileRequest {
	tileOffset := req.StartIndex % s.tileSize // FIXME tileSize? Doc the difference between tileSize and maxLeavesByRange or that it's used for two meanings?
	tileStart := req.StartIndex - tileOffset

	tiledReq := proto.Clone(req).(*trillian.GetLeavesByRangeRequest)
	tiledReq.StartIndex = tileStart
	tiledReq.Count = s.tileSize

	return tileRequest{
		req:    tiledReq,
		treeID: tree.TreeId,
	}
}

// tileSource is a helper enum to indicate to the user whether the tile returned
// to them was found in S3 or in the CT log.
type tileSource string

const (
	sourceCTLog tileSource = "CT log"
	sourceS3    tileSource = "S3"
)

func (s *tiledLeavesByRangeServer) getAndCacheTile(ctx context.Context, tile tileRequest) (*trillian.GetLeavesByRangeResponse, tileSource, error) {
	contents, err := s.getFromS3(ctx, tile)
	if err == nil {
		return contents, sourceS3, nil
	}

	if !errors.Is(err, noSuchKey{}) {
		return nil, sourceS3, fmt.Errorf("error reading tile from s3: %w", err)
	}

	contents, err = s.logServer.GetLeavesByRange(ctx, tile.req)
	if err != nil {
		return nil, sourceCTLog, fmt.Errorf("error reading tile from backend: %w", err)
	}

	// If we got a partial tile, assume we are at the end of the log and the last
	// tile isn't filled up yet. In that case, don't write to S3, but still return
	// results to the user.
	// FIXME can we tell we're at the end of the log with the AdminStorage or LogStorage interfaces?
	if s.isPartialTile(contents) {
		return contents, sourceCTLog, nil
	}

	err = s.writeToS3(ctx, tile, contents)
	if err != nil {
		return nil, sourceCTLog, fmt.Errorf("error writing tile to S3: %w", err)
	}
	return contents, sourceCTLog, nil
}

// isPartialTile returns true if there are fewer items in the tile than were
// requested by the tileCachingHandler.
func (s *tiledLeavesByRangeServer) isPartialTile(resp *trillian.GetLeavesByRangeResponse) bool {
	return int64(len(resp.Leaves)) < s.tileSize
}

func (s *tiledLeavesByRangeServer) getFromS3(ctx context.Context, t tileRequest) (*trillian.GetLeavesByRangeResponse, error) {
	key := s.s3Prefix + t.key()
	s3Resp, err := s.s3Service.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.s3Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, noSuchKey{}
		}
		return nil, fmt.Errorf("getting from bucket %q with key %q: %w", s.s3Bucket, key, err)
	}

	var ctResp trillian.GetLeavesByRangeResponse
	gzipReader, err := gzip.NewReader(s3Resp.Body)
	if err != nil {
		return nil, fmt.Errorf("making gzipReader: %w", err)
	}
	err = cbor.NewDecoder(gzipReader).Decode(&ctResp)
	if err != nil {
		return nil, fmt.Errorf("reading body from bucket %q with key %q: %w", s.s3Bucket, key, err)
	}

	if len(ctResp.Leaves) != int(t.req.Count) { // FIXME removed the t.end check here
		return nil, fmt.Errorf("internal inconsistency: len(entries) == %d; tileRequest = %v", len(ctResp.Leaves), t)
	}

	return &ctResp, nil
}

func (s *tiledLeavesByRangeServer) writeToS3(ctx context.Context, t tileRequest, ctResp *trillian.GetLeavesByRangeResponse) error {
	if len(ctResp.Leaves) != int(t.req.Count) { // FIXME removed the t.end check here too
		return fmt.Errorf("internal inconsistency: len(entries) == %d; tileRequest = %v", len(ctResp.Leaves), t)
	}

	var body bytes.Buffer
	w := gzip.NewWriter(&body)
	err := cbor.NewEncoder(w).Encode(ctResp)
	if err != nil {
		return nil
	}

	err = w.Close()
	if err != nil {
		return fmt.Errorf("closing gzip writer: %w", err)
	}

	key := s.s3Prefix + t.key()
	_, err = s.s3Service.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.s3Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body.Bytes()),
	})
	if err != nil {
		return fmt.Errorf("putting in bucket %q with key %q: %s", s.s3Bucket, key, err)
	}
	return nil
}

// noSuchKey indicates the requested key does not exist.
type noSuchKey struct{}

func (noSuchKey) Error() string {
	return "no such key"
}
