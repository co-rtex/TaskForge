// Package sqsbroker implements queue.Broker against any SQS-compatible service.
//
// The same code path serves ElasticMQ locally and AWS SQS in a deployment; only
// the endpoint and credentials differ. See
// docs/adr/0005-elasticmq-for-local-broker.md.
package sqsbroker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/co-rtex/TaskForge/internal/config"
	"github.com/co-rtex/TaskForge/internal/queue"
)

// Options configures a broker client.
type Options struct {
	// Endpoint overrides the AWS endpoint. Empty means real AWS.
	Endpoint        string
	Region          string
	QueueName       string
	AccessKeyID     string
	SecretAccessKey string
}

// OptionsFromConfig is the one mapping from validated configuration to a broker
// client, so every binary that builds one (taskforge-outbox, taskforge-worker)
// reads the same resolved values. With TASKFORGE_AWS_DEFAULT_CHAIN true the
// endpoint and keys are "", which New reads as "the SDK's defaults".
func OptionsFromConfig(cfg config.Config) Options {
	return Options{
		Endpoint:        cfg.BrokerEndpoint,
		Region:          cfg.BrokerRegion,
		QueueName:       cfg.BrokerQueueName,
		AccessKeyID:     cfg.BrokerAccessKeyID,
		SecretAccessKey: cfg.BrokerSecretAccessKey,
	}
}

// Broker is an SQS-compatible queue.Broker.
type Broker struct {
	client   *sqs.Client
	queueURL string
}

var _ queue.Broker = (*Broker)(nil)

// New resolves the queue URL once at construction, so a misnamed or missing
// queue fails at startup rather than on the first publish.
func New(ctx context.Context, opts Options) (*Broker, error) {
	if opts.QueueName == "" {
		return nil, fmt.Errorf("queue name is required")
	}
	client, err := newClient(ctx, opts)
	if err != nil {
		return nil, err
	}

	out, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(opts.QueueName)})
	if err != nil {
		return nil, fmt.Errorf("resolve queue url for %q: %w", opts.QueueName, err)
	}
	return &Broker{client: client, queueURL: aws.ToString(out.QueueUrl)}, nil
}

// newClient builds the SQS client New uses, and makes no network call. It is
// split from New so a test can read the client's resolved options without the
// GetQueueUrl round trip; New's behavior is unchanged.
func newClient(ctx context.Context, opts Options) (*sqs.Client, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(opts.Region)}
	if opts.AccessKeyID != "" {
		// ElasticMQ ignores credentials, but the SDK refuses to sign without
		// them. Real deployments leave these empty and use the default chain
		// (instance role, OIDC, or environment).
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(opts.AccessKeyID, opts.SecretAccessKey, ""),
		))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
		}
	}), nil
}

// QueueURL reports the resolved queue URL. Useful for logs and tests.
func (b *Broker) QueueURL() string { return b.queueURL }

// Publish sends one notification body.
func (b *Broker) Publish(ctx context.Context, body []byte) error {
	_, err := b.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(b.queueURL),
		MessageBody: aws.String(string(body)),
	})
	if err != nil {
		return fmt.Errorf("send message: %w", err)
	}
	return nil
}

// Receive long-polls for messages.
func (b *Broker) Receive(ctx context.Context, max int, wait time.Duration) ([]queue.Message, error) {
	if max < 1 {
		max = 1
	}
	if max > 10 { // SQS hard limit
		max = 10
	}
	out, err := b.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(b.queueURL),
		MaxNumberOfMessages: int32(max),
		WaitTimeSeconds:     int32(wait / time.Second),
	})
	if err != nil {
		return nil, fmt.Errorf("receive messages: %w", err)
	}

	msgs := make([]queue.Message, 0, len(out.Messages))
	for _, m := range out.Messages {
		msgs = append(msgs, queue.Message{
			ID:            aws.ToString(m.MessageId),
			Body:          []byte(aws.ToString(m.Body)),
			ReceiptHandle: aws.ToString(m.ReceiptHandle),
		})
	}
	return msgs, nil
}

// Delete acknowledges a delivery.
func (b *Broker) Delete(ctx context.Context, receiptHandle string) error {
	_, err := b.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(b.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return fmt.Errorf("delete message: %w", err)
	}
	return nil
}

// Ping checks broker reachability within the caller's deadline.
func (b *Broker) Ping(ctx context.Context) error {
	_, err := b.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(b.queueURL),
		AttributeNames: nil,
	})
	if err != nil {
		return fmt.Errorf("ping broker: %w", err)
	}
	return nil
}

// templateAttributes are the base queue's attributes a per-address queue
// inherits, so an operator configures the family once, on the base queue, and
// every address's queue matches it: the visibility timeout every claim decision
// is timed against, retention, delivery delay, long-poll default, size limit,
// and server-side encryption. A redrive policy is deliberately not copied: a
// notification is advisory, and a dead-letter queue would hold copies nothing
// claims. Only the attributes the broker reports are copied, so a broker that
// has no encryption attributes (ElasticMQ) is not asked to set them.
var templateAttributes = []string{
	"VisibilityTimeout",
	"MessageRetentionPeriod",
	"DelaySeconds",
	"ReceiveMessageWaitTimeSeconds",
	"MaximumMessageSize",
	"SqsManagedSseEnabled",
	"KmsMasterKeyId",
	"KmsDataKeyReusePeriodSeconds",
}

// Router is an SQS-compatible queue.Router: one broker queue per
// queue.Address, named by queue.NotificationQueueName from the configured base
// queue, which must exist and serves as the family's template and readiness
// target (docs/adr/0028-scope-addressed-notification-streams.md).
type Router struct {
	base     *Broker
	baseName string

	mu     sync.Mutex
	routes map[queue.Address]*Broker
}

var _ queue.Router = (*Router)(nil)

// NewRouter resolves the base queue at construction, exactly as New does, so a
// misnamed or missing base queue still fails at startup.
func NewRouter(ctx context.Context, opts Options) (*Router, error) {
	base, err := New(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Router{base: base, baseName: opts.QueueName, routes: map[queue.Address]*Broker{}}, nil
}

// Route returns address's broker queue, creating it from the base queue's
// attributes the first time any process needs it. Both the publisher and a
// worker may be first, so creation is idempotent: a queue that already exists
// is resolved, and a creation that lost a race to another process resolves the
// winner's queue.
func (r *Router) Route(ctx context.Context, address queue.Address) (queue.Broker, error) {
	if address.Scope == "" || address.Queue == "" {
		return nil, fmt.Errorf("route notification: scope and queue are both required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok := r.routes[address]; ok {
		return b, nil
	}
	name := queue.NotificationQueueName(r.baseName, address)
	url, err := r.resolveOrCreate(ctx, name)
	if err != nil {
		return nil, err
	}
	b := &Broker{client: r.base.client, queueURL: url}
	r.routes[address] = b
	return b, nil
}

func (r *Router) resolveOrCreate(ctx context.Context, name string) (string, error) {
	if out, err := r.base.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)}); err == nil {
		return aws.ToString(out.QueueUrl), nil
	}
	// Any lookup failure falls through to creation rather than being
	// classified: CreateQueue either creates the queue, returns the existing
	// one, or fails for a reason that is then reported.
	template, err := r.base.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(r.base.queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil {
		return "", fmt.Errorf("read base queue attributes for %q: %w", name, err)
	}
	attributes := map[string]string{}
	for _, attribute := range templateAttributes {
		if value, ok := template.Attributes[attribute]; ok {
			attributes[attribute] = value
		}
	}
	out, createErr := r.base.client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(name), Attributes: attributes,
	})
	if createErr == nil {
		return aws.ToString(out.QueueUrl), nil
	}
	if out, err := r.base.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)}); err == nil {
		return aws.ToString(out.QueueUrl), nil
	}
	return "", fmt.Errorf("create notification queue %q: %w", name, createErr)
}

// Ping checks the base queue, which every address's queue is created from.
func (r *Router) Ping(ctx context.Context) error { return r.base.Ping(ctx) }

// BaseQueueURL reports the base queue's URL. Useful for logs and tests.
func (r *Router) BaseQueueURL() string { return r.base.queueURL }
