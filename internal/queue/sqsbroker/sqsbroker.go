// Package sqsbroker implements queue.Broker against any SQS-compatible service.
//
// The same code path serves ElasticMQ locally and AWS SQS in a deployment; only
// the endpoint and credentials differ. See
// docs/adr/0005-elasticmq-for-local-broker.md.
package sqsbroker

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

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

	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
		}
	})

	out, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(opts.QueueName)})
	if err != nil {
		return nil, fmt.Errorf("resolve queue url for %q: %w", opts.QueueName, err)
	}
	return &Broker{client: client, queueURL: aws.ToString(out.QueueUrl)}, nil
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
