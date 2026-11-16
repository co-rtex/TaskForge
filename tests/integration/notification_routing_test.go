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
			require.NoError(t, err)
			urls <- b.(*sqsbroker.Broker).QueueURL()
		}(router)
	}
	close(start)
	wg.Wait()
	close(urls)
	var seen []string
	for u := range urls {
		seen = append(seen, u)
	}
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
