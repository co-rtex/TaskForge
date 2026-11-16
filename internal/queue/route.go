package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
)

// Address names one notification stream: the work.available notifications of
// one authorization scope's one TaskForge queue.
//
// It is the unit of claim fungibility. A claim selects among the due jobs of
// exactly one (scope, queue) pair (workers.Store.Claim), so any notification
// addressed to that pair can usefully wake any worker that reads it, and no
// notification addressed elsewhere can. Delivering each address on a broker
// queue of its own is what makes "safe to acknowledge relative to the claiming
// worker" coincide with "safe to acknowledge relative to the notification"
// (docs/adr/0028-scope-addressed-notification-streams.md).
type Address struct {
	Scope string
	Queue string
}

// Router resolves the broker queue that carries one Address's notifications.
//
// The outbox publisher routes every event by the scope and queue of the job it
// advertises; a worker resolves its own address once its session tells it its
// scope. Both sides derive the same broker queue from the same configured base
// name, so they meet without either telling the other anything.
type Router interface {
	Route(ctx context.Context, address Address) (Broker, error)
	// Ping checks reachability within the caller's deadline, as Broker.Ping.
	Ping(ctx context.Context) error
}

// MaxBaseQueueNameLength is the longest base name NotificationQueueName can
// extend and stay inside SQS's 80-character queue-name limit.
const MaxBaseQueueNameLength = 80 - 1 - addressDigestLength

// addressDigestLength is the hex length of the address digest: 64 bits.
const addressDigestLength = 16

var baseQueueNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// ValidateBaseQueueName reports whether base can name a family of standard SQS
// queues: SQS's character set, and short enough to carry an address suffix.
func ValidateBaseQueueName(base string) error {
	if !baseQueueNamePattern.MatchString(base) {
		return fmt.Errorf("must contain only letters, digits, hyphens and underscores")
	}
	if len(base) > MaxBaseQueueNameLength {
		return fmt.Errorf("must be at most %d characters, so the per-address suffix fits SQS's 80", MaxBaseQueueNameLength)
	}
	return nil
}

// NotificationQueueName is the broker queue that carries address's
// notifications: base, a hyphen, and the first 64 bits of a SHA-256 digest of
// the address, in hex.
//
// A digest rather than the scope itself, because a scope is up to 128
// characters of arbitrary text and an SQS name allows 80 of a narrow alphabet,
// and because infrastructure names are listed by people who need not learn
// tenant names from them. The NUL separator keeps ("a", "b-c") and ("a-b", "c")
// apart; NUL cannot occur in a scope or a queue name.
//
// Correctness does not depend on the digest being collision-free. Two
// addresses sharing a broker queue is exactly the pre-ADR-0028 arrangement,
// and the claim's foreign-notification guard keeps that arrangement from
// deleting another address's notification; a collision costs latency, not a
// job. At 64 bits it needs about four billion addresses to become likely.
func NotificationQueueName(base string, address Address) string {
	sum := sha256.Sum256([]byte(address.Scope + "\x00" + address.Queue))
	return base + "-" + hex.EncodeToString(sum[:])[:addressDigestLength]
}
