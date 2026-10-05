package stack

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// --- the run's own broker queue ------------------------------------------------

var queueURLPattern = regexp.MustCompile(`<QueueUrl>([^<]+)</QueueUrl>`)

// sqsCall issues one unsigned SQS query-protocol request. ElasticMQ, the local
// broker (ADR-0005), accepts it; the AWS SDK clients the services use are not
// needed to create and delete a queue.
func sqsCall(ctx context.Context, endpoint string, form url.Values) (string, error) {
	form.Set("Version", "2012-11-05")
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<16))
	if err != nil {
		return "", err
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s: HTTP %d: %s", form.Get("Action"), response.StatusCode, body)
	}
	return string(body), nil
}

// createQueue creates the queue named name and returns its URL.
func createQueue(ctx context.Context, endpoint, name string) (string, error) {
	body, err := sqsCall(ctx, endpoint, url.Values{"Action": {"CreateQueue"}, "QueueName": {name}})
	if err != nil {
		return "", err
	}
	match := queueURLPattern.FindStringSubmatch(body)
	if len(match) != 2 {
		return "", fmt.Errorf("CreateQueue returned no queue URL: %s", body)
	}
	return match[1], nil
}

// deleteQueue deletes a queue this run created.
func deleteQueue(ctx context.Context, endpoint, queueURL string) error {
	_, err := sqsCall(ctx, endpoint, url.Values{"Action": {"DeleteQueue"}, "QueueUrl": {queueURL}})
	return err
}
