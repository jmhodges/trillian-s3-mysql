package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/trillian"
	"github.com/google/trillian/extension"
	"github.com/google/trillian/monitoring"
	"github.com/google/trillian/trees"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

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
	ctx, spanEnd := spanFor(ctx, "TiledGetLeavesByRange")
	defer spanEnd()

	err := validateGetLeavesByRangeRequest(req)
	if err != nil {
		return nil, err
	}

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

func validateGetLeavesByRangeRequest(req *trillian.GetLeavesByRangeRequest) error {
	if req.StartIndex < 0 {
		return status.Errorf(codes.InvalidArgument, "GetLeavesByRangeRequest.StartIndex: %v, want >= 0", req.StartIndex)
	}
	if req.Count <= 0 {
		return status.Errorf(codes.InvalidArgument, "GetLeavesByRangeRequest.Count: %v, want > 0", req.Count)
	}
	return nil
}

const traceSpanRoot = "/trillian"

func spanFor(ctx context.Context, name string) (context.Context, func()) {
	return monitoring.StartSpan(ctx, fmt.Sprintf("%s.%s", traceSpanRoot, name))
}
