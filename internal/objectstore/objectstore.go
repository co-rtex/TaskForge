// Package objectstore is a thin S3-compatible client for large result bytes.
//
// It carries no domain knowledge of jobs, attempts, or scopes -- see
// internal/results for that. The same code path serves MinIO locally and AWS
// S3 in a deployment; only the endpoint and credentials differ, exactly as
// internal/queue/sqsbroker serves ElasticMQ and AWS SQS. See
// docs/adr/0015-result-storage.md.
package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Options configures an object-store client.
type Options struct {
	// Endpoint overrides the AWS endpoint. Empty means real AWS.
	Endpoint string
	Region   string
	// Bucket is ensured to exist at construction, so a misconfigured or
	// unreachable bucket fails at startup rather than on the first result
	// upload -- the same reasoning sqsbroker.New resolves its queue URL for.
	Bucket          string
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

// New ensures the configured bucket exists and returns a client.
func New(ctx context.Context, opts Options) (*Client, error) {
	if opts.Bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(opts.Region)}
	if opts.AccessKeyID != "" {
		// MinIO ignores credentials it doesn't recognize, but the SDK refuses to
		// sign without them. Real deployments leave these empty and use the
		// default chain (instance role, OIDC, or environment).
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(opts.AccessKeyID, opts.SecretAccessKey, ""),
		))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
			// MinIO does not support virtual-hosted-style addressing for
			// arbitrary bucket names; real AWS S3 needs no such override.
			o.UsePathStyle = true
		}
	})

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(opts.Bucket)}); err != nil {
		var ownedByYou *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if !errors.As(err, &ownedByYou) && !errors.As(err, &exists) {
			return nil, fmt.Errorf("ensure results bucket %q exists: %w", opts.Bucket, err)
		}
	}

	return &Client{client: client}, nil
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
