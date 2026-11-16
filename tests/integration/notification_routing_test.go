//go:build integration

package integration

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/queue"
	"github.com/co-rtex/TaskForge/internal/queue/sqsbroker"
)

var attributePattern = regexp.MustCompile(`<Attribute>\s*<Name>([^<]+)</Name>\s*<Value>([^<]*)</Value>\s*</Attribute>`)

// brokerQueueAttribute reads one attribute of a broker queue.
func brokerQueueAttribute(t *testing.T, queueURL, name string) string {
	t.Helper()
	response, err := http.PostForm(brokerEndpoint()+"/", url.Values{
		"Action": {"GetQueueAttributes"}, "QueueUrl": {queueURL},
		"AttributeName.1": {"All"}, "Version": {"2012-11-05"},
	})
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, response.StatusCode, "GetQueueAttributes failed: %s", body)
	for _, match := range attributePattern.FindAllStringSubmatch(string(body), -1) {
		if match[1] == name {
			return match[2]
		}
	}
	t.Fatalf("queue %s reports no %s: %s", queueURL, name, body)
	return ""
}

// TestRouting_AnAddressQueueInheritsTheBaseQueuesAttributes: the operator
// configures the family once, on the base queue, and the visibility timeout
// every held notification is timed by must be the base's, not the broker's
// default.
func TestRouting_AnAddressQueueInheritsTheBaseQueuesAttributes(t *testing.T) {
	router := newIsolatedRouter(t, "taskforge-route-")
	_, err := http.PostForm(brokerEndpoint()+"/", url.Values{
		"Action": {"SetQueueAttributes"}, "QueueUrl": {router.BaseQueueURL()},
		"Attribute.1.Name": {"VisibilityTimeout"}, "Attribute.1.Value": {"7"},
		"Version": {"2012-11-05"},
	})
	require.NoError(t, err)
	require.Equal(t, "7", brokerQueueAttribute(t, router.BaseQueueURL(), "VisibilityTimeout"))

	derived := router.address(t, "tenant-a", "default")
	require.NotEqual(t, router.BaseQueueURL(), derived.QueueURL())
	require.Equal(t, "7", brokerQueueAttribute(t, derived.QueueURL(), "VisibilityTimeout"))
}

// TestRouting_IndependentProcessesMeetOnOneQueuePerAddress: the publisher and a
// worker resolve an address independently and concurrently, and whichever
// creates the queue, both get the same one; another scope gets another.
func TestRouting_IndependentProcessesMeetOnOneQueuePerAddress(t *testing.T) {
	first := newIsolatedRouter(t, "taskforge-route-")
	opts := brokerOptions()
	opts.QueueName = queueNameOf(t, first.BaseQueueURL())

	const racers = 6
	address := queue.Address{Scope: "tenant-a", Queue: "default"}
	urls := make(chan string, racers)
	errs := make(chan error, racers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		router, err := sqsbroker.NewRouter(context.Background(), opts)
		require.NoError(t, err)
		wg.Add(1)
		go func(router *sqsbroker.Router) {
			defer wg.Done()
			<-start
			b, err := router.Route(context.Background(), address)
			if err != nil {
				errs <- err
				return
			}
			urls <- b.(*sqsbroker.Broker).QueueURL()
		}(router)
	}
	close(start)
	wg.Wait()
	close(urls)
	close(errs)
	for err := range errs {
		require.NoError(t, err, "a racing creator must resolve the queue, not fail")
	}
	var seen []string
	for u := range urls {
		seen = append(seen, u)
	}
	require.Len(t, seen, racers)
	for _, u := range seen {
		require.Equal(t, seen[0], u, "every process must resolve the same queue for one address")
	}
	require.Equal(t, seen[0], first.address(t, "tenant-a", "default").QueueURL())

	// A message published through one router is received through another.
	publisher := first.address(t, "tenant-a", "default")
	require.NoError(t, publisher.Publish(context.Background(), []byte(`{"probe":true}`)))
	other, err := sqsbroker.NewRouter(context.Background(), opts)
	require.NoError(t, err)
	consumer, err := other.Route(context.Background(), address)
	require.NoError(t, err)
	bodies := receiveAll(t, consumer, 500*time.Millisecond)
	require.Equal(t, [][]byte{[]byte(`{"probe":true}`)}, bodies)

	// And another scope's address is a different queue that saw nothing.
	elsewhere := first.address(t, "tenant-b", "default")
	require.NotEqual(t, seen[0], elsewhere.QueueURL())
	require.Empty(t, receiveAll(t, elsewhere, 300*time.Millisecond))
}

func queueNameOf(t *testing.T, queueURL string) string {
	t.Helper()
	parsed, err := url.Parse(queueURL)
	require.NoError(t, err)
	return path.Base(parsed.Path)
}

// TestRouting_AnAddressQueueThatDisappearsIsRecreated: a broker restart (the
// local ElasticMQ keeps queues in memory and recreates only the configured
// base) or an operator's deletion removes an address's queue after the
// publisher and a worker resolved it. Before ADR-0028 the one queue came back
// with the broker; an address's queue must come back too, through the handles
// both sides already hold, or the publisher fails every event and the worker
// every receive until each process restarts.
func TestRouting_AnAddressQueueThatDisappearsIsRecreated(t *testing.T) {
	router := newIsolatedRouter(t, "taskforge-route-")
	publisher := router.address(t, "tenant-a", "default")
	opts := brokerOptions()
	opts.QueueName = queueNameOf(t, router.BaseQueueURL())
	workerSide, err := sqsbroker.NewRouter(context.Background(), opts)
	require.NoError(t, err)
	consumer, err := workerSide.Route(context.Background(), queue.Address{Scope: "tenant-a", Queue: "default"})
	require.NoError(t, err)

	deleteBrokerQueue(publisher.QueueURL())

	require.NoError(t, publisher.Publish(context.Background(), []byte(`{"after":"recreate"}`)),
		"publishing to a vanished address queue must recreate it, not fail until restart")
	require.Equal(t, [][]byte{[]byte(`{"after":"recreate"}`)}, receiveAll(t, consumer, 500*time.Millisecond),
		"a worker's existing handle must reach the recreated queue")
}

// TestRouting_AnAddressResolvesOnceTheBrokerIsBack: resolving an address while
// the broker is unreachable fails and caches nothing, so the publisher's next
// attempt (its own backoff) resolves the queue once the broker answers again.
func TestRouting_AnAddressResolvesOnceTheBrokerIsBack(t *testing.T) {
	base := newIsolatedRouter(t, "taskforge-route-")
	proxy := newFlakyProxy(t, brokerEndpoint())
	opts := brokerOptions()
	opts.Endpoint = proxy.URL()
	opts.QueueName = queueNameOf(t, base.BaseQueueURL())
	router, err := sqsbroker.NewRouter(context.Background(), opts)
	require.NoError(t, err)
	address := queue.Address{Scope: "tenant-a", Queue: "default"}

	proxy.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err = router.Route(ctx, address)
	require.Error(t, err, "an unreachable broker cannot resolve an address")

	proxy.Start()
	b, err := router.Route(context.Background(), address)
	require.NoError(t, err, "a failed resolution must not be cached")
	require.NoError(t, b.Publish(context.Background(), []byte(`{"back":true}`)))
	require.Equal(t, [][]byte{[]byte(`{"back":true}`)},
		receiveAll(t, base.address(t, "tenant-a", "default"), 500*time.Millisecond))
}
