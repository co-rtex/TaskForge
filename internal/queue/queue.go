// Package queue defines TaskForge's provider-neutral broker abstraction.
//
// The broker is a notification channel, never authoritative state
// (docs/adr/0003-pull-based-claim-with-broker-notification.md). The interface
// therefore exposes only what TaskForge actually needs: publish, long-poll
// receive, and acknowledge.
//
// There is deliberately no Nack. SQS has no such primitive — redelivery is
// expressed by letting a message's visibility timeout expire — and defining one
// here would create an abstraction that cannot be honored in production.
package queue

import (
	"context"
	"time"
)

// Message is one received broker message.
type Message struct {
	// ID is broker-assigned and useful only for logging. It is not a TaskForge
	// identifier and carries no authority.
	ID string

	Body []byte

	// ReceiptHandle acknowledges this particular delivery. A redelivery of the
	// same message has a different handle.
	ReceiptHandle string
}

// Publisher sends a notification.
type Publisher interface {
	Publish(ctx context.Context, body []byte) error
}

// Receiver consumes notifications.
type Receiver interface {
	// Receive long-polls for up to max messages, returning early once any are
	// available. An empty result is normal, not an error.
	Receive(ctx context.Context, max int, wait time.Duration) ([]Message, error)

	// Delete acknowledges a message so it is not redelivered.
	Delete(ctx context.Context, receiptHandle string) error
}

// Broker is the full local capability set. Ping supports readiness probes and
// must respect the caller's context deadline.
type Broker interface {
	Publisher
	Receiver
	Ping(ctx context.Context) error
}
