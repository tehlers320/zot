//go:build sync

package sync

import (
	"context"
	"io"

	godigest "github.com/opencontainers/go-digest"
	"github.com/regclient/regclient/config"
	"github.com/regclient/regclient/types/blob"
	"github.com/regclient/regclient/types/descriptor"
	"github.com/regclient/regclient/types/manifest"

	zerr "zotregistry.dev/zot/v2/errors"
	"zotregistry.dev/zot/v2/pkg/extensions/sync/stream"
)

// This file wires the stream package's priority fetcher (see
// stream/priority_fetcher.go) into BaseService: the fetching engine lives in
// the stream package, while this adapter only supplies upstream access
// (repo resolution + a dedicated client) and the trigger entry points.

// streamPrioritizer is implemented by services that support priority fetching
// of streamed blobs. BaseOnDemand discovers it via a type assertion, so
// services (and test mocks) without it keep working unchanged.
type streamPrioritizer interface {
	PrioritizeBlobForStream(localRepo string, digest godigest.Digest)
	PrefetchManifestBlobsForStream(localRepo string, mfst manifest.Manifest)
}

// priorityFetchBudget returns the per-host request budget for the priority
// fetch path, derived from the registry's reqConcurrent (0 when unset, which
// makes the stream package fall back to its own default).
func (service *BaseService) priorityFetchBudget() int {
	if service.config.ReqConcurrent == nil {
		return 0
	}

	return *service.config.ReqConcurrent
}

// initPriorityFetcher creates the priority fetcher for streaming-enabled
// services. Called from New(); a no-op when streaming is disabled.
func (service *BaseService) initPriorityFetcher() {
	if service.streamManager == nil || !service.config.IsStreamEnabled() {
		return
	}

	service.priorityFetcher = stream.NewPriorityFetcher(
		service.streamManager, service.openPriorityBlob, service.GetSyncTimeout(),
		service.priorityFetchBudget(), service.log)
}

// initPriorityClient (re)builds the dedicated upstream client for priority
// fetches, with its own per-host request budget so priority downloads never
// queue behind the bulk ImageCopy downloads of the main client. Called from
// initClient with clientLock held.
func (service *BaseService) initPriorityClient() error {
	if !service.config.IsStreamEnabled() {
		return nil
	}

	budget := stream.PriorityFetchHostBudget(service.priorityFetchBudget())

	prioClient, _, err := newClient(service.config, service.credentials, service.log,
		func(host *config.Host) {
			host.ReqConcurrent = budget
		})
	if err != nil {
		service.log.Err(err).Msg("failed to create priority fetch client")

		return err
	}

	service.prioClient = prioClient

	return nil
}

// openPriorityBlob opens an upstream reader for a blob through the priority
// client. It is the stream.OpenBlobFunc supplied to the priority fetcher.
func (service *BaseService) openPriorityBlob(ctx context.Context, localRepo string,
	desc descriptor.Descriptor,
) (*blob.BReader, error) {
	remoteRepo := localRepo
	if len(service.config.Content) > 0 {
		remoteRepo = service.contentManager.GetRepoSource(localRepo)
		if remoteRepo == "" {
			return nil, zerr.ErrSyncImageFilteredOut
		}
	}

	service.clientLock.RLock()
	prioClient := service.prioClient
	remote := service.remote
	service.clientLock.RUnlock()

	if prioClient == nil || remote == nil {
		return nil, zerr.ErrSyncUpstreamDownloadFailed
	}

	imageRef, err := remote.GetImageReference(remoteRepo, desc.Digest.String())
	if err != nil {
		return nil, err
	}

	// blob.Reader is an alias for *blob.BReader in regclient.
	return prioClient.BlobGet(ctx, imageRef, desc)
}

// OpenBlobForStream opens a blob directly from the configured upstream. This
// is the cross-pod fallback for streaming pulls: another replica can own the
// in-memory stream while this replica receives the client's blob request.
func (service *BaseService) OpenBlobForStream(ctx context.Context, localRepo string,
	digest godigest.Digest,
) (io.ReadCloser, int64, error) {
	reader, err := service.openPriorityBlob(ctx, localRepo, descriptor.Descriptor{Digest: digest})
	if err != nil {
		return nil, 0, err
	}

	return reader, reader.GetDescriptor().Size, nil
}

// PrioritizeBlobForStream starts an out-of-band upstream download for a blob a
// downstream client is waiting on. No-op when streaming is disabled.
func (service *BaseService) PrioritizeBlobForStream(localRepo string, digest godigest.Digest) {
	if service.priorityFetcher == nil {
		return
	}

	service.priorityFetcher.PrioritizeBlob(localRepo, digest)
}

// PrefetchManifestBlobsForStream schedules priority downloads for the blobs of
// a platform manifest a client resolved by digest. No-op when streaming is
// disabled.
func (service *BaseService) PrefetchManifestBlobsForStream(localRepo string, mfst manifest.Manifest) {
	if service.priorityFetcher == nil {
		return
	}

	service.priorityFetcher.PrefetchManifestBlobs(localRepo, mfst)
}
