// Package objectstore is a thin S3-compatible client for large result bytes.
//
// It carries no domain knowledge of jobs, attempts, or scopes -- see
// internal/results for that. The same code path serves LocalStack's S3
// provider locally and AWS S3 in a deployment; only the endpoint and
// credentials differ, exactly as internal/queue/sqsbroker serves ElasticMQ
// and AWS SQS. See docs/adr/0015-result-storage.md.
package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// The SDK's own default HTTP client (awshttp.BuildableClient, below) leaves
// its overall Timeout at the zero value -- unbounded -- unless a caller sets
// one; only its per-phase Expect-100-Continue wait is bounded by default.
// Without this, an object store that accepts a connection but never answers
// hangs the request forever, holding the worker's concurrency slot along
// with it -- confirmed against this project's own CI, where LocalStack does
// exactly that against every PutObject and an unbounded client produced no
// error at all, only a silent hang indistinguishable from any other cause of
// a slow attempt.
//
// The bound differs by destination because the failure mode does too. A
// local, same-host endpoint has no legitimate reason to take seconds to
// answer even a large body; a bound generous enough for real AWS S3 over a
// real network would just make a genuinely wedged local endpoint hang for
// most of a test's own patience before ever producing a visible error.
// remoteRequestTimeout is not yet exercised by anything in this repository
// (no deployment targets real AWS S3 yet) and is a placeholder bound
// pending real production traffic to tune it against, not a measured
// value.
const (
	localRequestTimeout  = 3 * time.Second
	remoteRequestTimeout = 30 * time.Second
)

// Options configures an object-store client.
type Options struct {
	// Endpoint overrides the AWS endpoint. Empty means real AWS.
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
}

// Client is a bucket-agnostic S3-compatible client. Bucket is a parameter on
// every call rather than fixed at construction, because internal/results
// reads it back from the results table, which is the authoritative record of
// where a given result actually lives.
type Client struct {
	client *s3.Client
}

// New builds a client. It makes no network call itself: a read-only caller
// (taskforge-api) can construct one without its process boot depending on
// the object store being reachable, since only a request that actually
// needs an object-located result ever touches it. A writer
// (taskforge-worker) should call EnsureBucket once, separately, before it
// starts accepting work.
func New(ctx context.Context, opts Options) (*Client, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(opts.Region)}
	if opts.AccessKeyID != "" {
		// LocalStack ignores credentials it doesn't recognize, but the SDK
		// refuses to sign without them. Real deployments leave these empty and
		// use the default chain (instance role, OIDC, or environment).
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(opts.AccessKeyID, opts.SecretAccessKey, ""),
		))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// WithTimeout keeps every other BuildableClient default (proxy from
		// environment, TLS 1.2 minimum, connection pooling) and adds only the
		// one missing bound; see the constants' comment above for why it
		// exists and why the local and remote bounds differ.
		timeout := remoteRequestTimeout
		if opts.Endpoint != "" {
			timeout = localRequestTimeout
		}
		o.HTTPClient = awshttp.NewBuildableClient().WithTimeout(timeout)
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
			// A local endpoint needs path-style addressing for an arbitrary
			// bucket name -- LocalStack does not support virtual-hosted-style
			// requests against it; real AWS S3 needs no such override.
			o.UsePathStyle = true
			// The SDK's default since v1.30 attaches a trailing CRC32
			// checksum to every PutObject via aws-chunked transfer encoding
			// -- framing some S3-compatible servers cannot parse. That path
			// only ever engages over HTTPS (the SDK's checksum middleware
			// checks req.IsHTTPS() before switching to a trailing checksum),
			// so it never explains a hang against a plain-HTTP local
			// endpoint; it is set here as defense in depth for a local
			// endpoint later fronted by TLS, matching the pre-v1.30 behavior
			// of computing a checksum only for operations that require one
			// rather than every operation that merely supports one. Real AWS
			// S3 keeps the modern default. See localRequestTimeout above for
			// what actually bounds a request against an endpoint that
			// accepts a connection but never answers.
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		}
	})

	return &Client{client: client}, nil
}

// EnsureBucket creates bucket if it does not already exist, tolerating a
// concurrent creator. Call this once, before the first Put, so a
// misconfigured or unreachable object store fails at worker startup rather
// than on the first attempt to report a large result -- the same reasoning
// sqsbroker.New resolves its queue URL for.
func (c *Client) EnsureBucket(ctx context.Context, bucket string) error {
	if _, err := c.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		var ownedByYou *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if !errors.As(err, &ownedByYou) && !errors.As(err, &exists) {
			return fmt.Errorf("ensure results bucket %q exists: %w", bucket, err)
		}
	}
	return nil
}

// Put uploads body under bucket and key, replacing any existing object at
// that key. TaskForge never reads back an object it did not write, and every
// write today targets a deterministic key that already includes the
// attempt id that produced it (see internal/worker/runner.go), so this is
// never called twice for the same key with different bytes in practice.
func (c *Client) Put(ctx context.Context, bucket, key string, body []byte, contentType string) error {
	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("put object %s/%s: %w", bucket, key, err)
	}
	return nil
}

// ErrNotFound reports that bucket and key named no object.
var ErrNotFound = errors.New("object not found")

// Get downloads and returns the complete bytes stored at bucket and key.
func (c *Client) Get(ctx context.Context, bucket, key string) ([]byte, error) {
	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get object %s/%s: %w", bucket, key, err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read object %s/%s: %w", bucket, key, err)
	}
	return body, nil
}
