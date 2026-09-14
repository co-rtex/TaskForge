package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/auth"
	"github.com/co-rtex/TaskForge/internal/workerauth"
)

// testRawWorkerKey is a well-formed worker credential, shaped exactly like
// testRawKey but distinct from it: the two guard different trust boundaries
// and must never be interchangeable, even by accident in a test fixture.
var testRawWorkerKey = auth.KeyPrefix + strings.Repeat("c", auth.LookupLen) + "." + strings.Repeat("d", auth.SecretLen)

// fakeWorkerKeys is a worker-key credential store with no database behind it.
//
// It implements the whole WorkerKeys interface because that is what the
// Server wires, but every test here drives only the behavior it names.
type fakeWorkerKeys struct {
	authenticate func(context.Context, string) (workerauth.Principal, error)
	create       func(context.Context, string, string) (workerauth.Created, error)
	revoke       func(context.Context, uuid.UUID) (workerauth.RevokeResult, error)
	list         func(context.Context, int) ([]workerauth.Key, error)
	isRevoked    func(context.Context, uuid.UUID) (bool, error)
}

func (f *fakeWorkerKeys) Authenticate(ctx context.Context, raw string) (workerauth.Principal, error) {
	if f.authenticate == nil {
		return workerauth.Principal{}, workerauth.ErrUnauthorized
	}
	return f.authenticate(ctx, raw)
}

func (f *fakeWorkerKeys) Create(ctx context.Context, scope, name string) (workerauth.Created, error) {
	if f.create == nil {
		return workerauth.Created{}, workerauth.ErrUnauthorized
	}
	return f.create(ctx, scope, name)
}

func (f *fakeWorkerKeys) Revoke(ctx context.Context, id uuid.UUID) (workerauth.RevokeResult, error) {
	if f.revoke == nil {
		return workerauth.RevokeResult{}, workerauth.ErrKeyNotFound
	}
	return f.revoke(ctx, id)
}

func (f *fakeWorkerKeys) List(ctx context.Context, limit int) ([]workerauth.Key, error) {
	if f.list == nil {
		return nil, nil
	}
	return f.list(ctx, limit)
}

func (f *fakeWorkerKeys) IsRevoked(ctx context.Context, id uuid.UUID) (bool, error) {
	if f.isRevoked == nil {
		return false, nil
	}
	return f.isRevoked(ctx, id)
}

var _ WorkerKeys = (*fakeWorkerKeys)(nil)

// acceptingWorkerKeys accepts exactly testRawWorkerKey and nothing else,
// authenticating it to the given scope under a fresh key id.
func acceptingWorkerKeys(scope string) *fakeWorkerKeys {
	return &fakeWorkerKeys{
		authenticate: func(_ context.Context, raw string) (workerauth.Principal, error) {
			if raw != testRawWorkerKey {
				return workerauth.Principal{}, workerauth.ErrUnauthorized
			}
			return workerauth.Principal{KeyID: uuid.New(), Scope: scope}, nil
		},
	}
}

// authorizeWorker presents the credential acceptingWorkerKeys accepts.
func authorizeWorker(req *http.Request) *http.Request {
	req.Header.Set(authorizationHeader, "Bearer "+testRawWorkerKey)
	return req
}
