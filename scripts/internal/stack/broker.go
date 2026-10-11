package stack

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
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

// createQueue creates the queue named name and returns its URL. attributes are
// SQS queue attributes (VisibilityTimeout, ...); nil creates the queue with the
// broker's defaults, which is what every program but the recovery probe does.
func createQueue(ctx context.Context, endpoint, name string, attributes map[string]string) (string, error) {
	form := url.Values{"Action": {"CreateQueue"}, "QueueName": {name}}
	names := make([]string, 0, len(attributes))
	for attribute := range attributes {
		names = append(names, attribute)
	}
	sort.Strings(names)
	for i, attribute := range names {
		form.Set(fmt.Sprintf("Attribute.%d.Name", i+1), attribute)
		form.Set(fmt.Sprintf("Attribute.%d.Value", i+1), attributes[attribute])
	}
	body, err := sqsCall(ctx, endpoint, form)
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

var queueAttributePattern = regexp.MustCompile(`<Attribute>\s*<Name>([^<]+)</Name>\s*<Value>([^<]*)</Value>\s*</Attribute>`)

// queueAttributes returns every attribute GetQueueAttributes reports for a queue:
// among them ApproximateNumberOfMessages (visible), ApproximateNumberOfMessagesNotVisible
// (received and neither deleted nor returned yet) and VisibilityTimeout.
func queueAttributes(ctx context.Context, endpoint, queueURL string) (map[string]string, error) {
	body, err := sqsCall(ctx, endpoint, url.Values{
		"Action": {"GetQueueAttributes"}, "QueueUrl": {queueURL}, "AttributeName.1": {"All"},
	})
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, match := range queueAttributePattern.FindAllStringSubmatch(body, -1) {
		out[match[1]] = match[2]
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("GetQueueAttributes returned no attributes: %s", body)
	}
	return out, nil
}
