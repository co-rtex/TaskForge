package sqsbroker

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/config"
)

// These tests prove TASKFORGE_AWS_DEFAULT_CHAIN reaches the SQS client itself,
// not only the configuration: the client the binaries build, from the
// configuration the binaries load, through OptionsFromConfig, carries no
// endpoint override and no static credentials. They need no network: newClient
// stops before New's GetQueueUrl call, and nothing here retrieves a credential
// or resolves a host.

// hermetic keeps the developer's own AWS files and variables out of the SDK's
// default configuration loading.
func hermetic(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_CONFIG_FILE", os.DevNull)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", os.DevNull)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_SQS"} {
		unset(t, name)
	}
}

func unset(t *testing.T, name string) {
	t.Helper()
	t.Setenv(name, "")
	require.NoError(t, os.Unsetenv(name))
}

func clientFromEnvironment(t *testing.T) sqs.Options {
	t.Helper()
	cfg, err := config.Load()
	require.NoError(t, err)
	client, err := newClient(context.Background(), OptionsFromConfig(cfg))
	require.NoError(t, err)
	return client.Options()
}

// TestClient_AWSDefaultChainReachesTheClient: with the switch on, the client has
// no BaseEndpoint, its credentials are not a static provider, and the endpoint
// the SDK resolves for the region is the AWS regional one.
func TestClient_AWSDefaultChainReachesTheClient(t *testing.T) {
	hermetic(t)
	for _, name := range []string{
		"TASKFORGE_BROKER_ENDPOINT", "TASKFORGE_BROKER_ACCESS_KEY_ID", "TASKFORGE_BROKER_SECRET_ACCESS_KEY",
		"TASKFORGE_RESULTS_ENDPOINT", "TASKFORGE_RESULTS_ACCESS_KEY_ID", "TASKFORGE_RESULTS_SECRET_ACCESS_KEY",
	} {
		unset(t, name)
	}
	t.Setenv("TASKFORGE_AWS_DEFAULT_CHAIN", "true")
	t.Setenv("TASKFORGE_BROKER_REGION", "eu-west-1")
	t.Setenv("TASKFORGE_RESULTS_REGION", "eu-west-1")

	o := clientFromEnvironment(t)
	require.Nilf(t, o.BaseEndpoint, "no endpoint override: the SDK resolves it from the region; got %q", aws.ToString(o.BaseEndpoint))
	require.Equal(t, "eu-west-1", o.Region)
	require.NotNil(t, o.Credentials, "the SDK's default chain")
	require.False(t, aws.IsCredentialsProvider(o.Credentials, credentials.StaticCredentialsProvider{}),
		"no static credentials provider, so no \"local\" keys")

	endpoint, err := o.EndpointResolverV2.ResolveEndpoint(context.Background(), sqs.EndpointParameters{Region: aws.String(o.Region)})
	require.NoError(t, err)
	require.Equal(t, "sqs.eu-west-1.amazonaws.com", endpoint.URI.Host)
	require.Equal(t, "https", endpoint.URI.Scheme)
}

// TestClient_LocalDefaultsReachTheClient is the control: with the switch unset
// the same path gives the local ElasticMQ endpoint and static "local" keys, so
// the assertions above can tell the two apart.
func TestClient_LocalDefaultsReachTheClient(t *testing.T) {
	hermetic(t)
	for _, name := range []string{"TASKFORGE_AWS_DEFAULT_CHAIN", "TASKFORGE_BROKER_ENDPOINT", "TASKFORGE_BROKER_ACCESS_KEY_ID", "TASKFORGE_BROKER_SECRET_ACCESS_KEY", "TASKFORGE_BROKER_REGION"} {
		unset(t, name)
	}
	o := clientFromEnvironment(t)
	require.Equal(t, "http://127.0.0.1:9324", aws.ToString(o.BaseEndpoint))
	require.True(t, aws.IsCredentialsProvider(o.Credentials, credentials.StaticCredentialsProvider{}))
}
