//go:build sync

package sync

import (
	"context"
	"sync"

	godigest "github.com/opencontainers/go-digest"
	"github.com/regclient/regclient/types/descriptor"
	"github.com/regclient/regclient/types/manifest"
	"github.com/regclient/regclient/types/ref"

	zerr "zotregistry.dev/zot/v2/errors"
)

// defaultChildFetchConcurrency matches regclient's default per-host request budget,
// used when the registry config leaves ReqConcurrent unset.
const defaultChildFetchConcurrency = 3

// This file contains the streaming-sync specific behavior of BaseService.

// IsStreamingForRepo returns whether streaming is enabled for the given local repo on this service.
// Streaming is enabled if the registry config has Stream set to true and the repo matches the content config.
func (service *BaseService) IsStreamingForRepo(repo string) bool {
	if !service.config.IsStreamEnabled() {
		return false
	}

	// If no content filter is configured, all repos match.
	if len(service.config.Content) == 0 {
		return true
	}

	return service.contentManager.GetContentByLocalRepo(repo) != nil
}

// EnsureLocalRepo creates the local repo layout (blobs dir, oci-layout and an
// empty index.json) before a streamed manifest is served to the client.
// Streaming answers the manifest from upstream while the image is still being
// synced, so without this the repo has no index.json for the whole transfer and
// every lookup that reads it (referrers, tags) fails on a repo the client was
// just told exists. InitRepo is idempotent and does not make any tag or
// manifest visible.
func (service *BaseService) EnsureLocalRepo(ctx context.Context, repo string) error {
	return service.storeController.GetImageStore(repo).InitRepo(ctx, repo)
}

// localManifestIsUpToDate reports whether the locally stored manifest for
// repo:reference is still the one upstream serves, in which case the caller can
// answer from local storage instead of pulling the manifest (and, for an index,
// every child manifest) over the network.
//
// A digest reference is content-addressed, so having it locally is proof enough.
// For a tag, a single HEAD is compared against the local digest. The comparison
// is only ever used to *skip* work: any doubt - HEAD failing, an upstream that
// omits Docker-Content-Digest, a digest that differs for whatever reason -
// returns false and leaves the caller on the full fetch path.
func (service *BaseService) localManifestIsUpToDate(ctx context.Context, repo, reference string,
	artifactRef ref.Ref,
) bool {
	imageStore := service.storeController.GetImageStore(repo)
	if imageStore == nil {
		return false
	}

	_, localDigest, _, err := imageStore.GetImageManifest(repo, reference)
	if err != nil {
		return false
	}

	// Tags move, digests do not: a digest reference present locally cannot be stale.
	if refDigest, err := godigest.Parse(reference); err == nil {
		return refDigest == localDigest
	}

	upstreamManifest, err := service.rc.ManifestHead(ctx, artifactRef)
	if err != nil {
		service.log.Debug().Err(err).Str("repo", repo).Str("reference", reference).
			Msg("sync: upstream manifest HEAD failed, falling back to a full fetch")

		return false
	}

	upstreamDigest := upstreamManifest.GetDescriptor().Digest
	if upstreamDigest == "" {
		// Registries are required to answer HEAD with Docker-Content-Digest;
		// without it there is nothing to compare, so do the full fetch.
		service.log.Debug().Str("repo", repo).Str("reference", reference).
			Msg("sync: upstream manifest HEAD carried no digest, falling back to a full fetch")

		return false
	}

	return upstreamDigest == localDigest
}

// FetchManifest on demand.
func (service *BaseService) FetchManifest(ctx context.Context, repo, reference string) (
	manifest.Manifest, []manifest.Manifest, error,
) {
	remoteRepo := repo

	remoteURL := service.remote.GetHostName()

	if len(service.config.Content) > 0 {
		remoteRepo = service.contentManager.GetRepoSource(repo)
		if remoteRepo == "" {
			service.log.Info().Str("remote", remoteURL).Str("repo", repo).Str("reference", reference).
				Msg("will not sync image, filtered out by content")

			return nil, nil, zerr.ErrSyncImageFilteredOut
		}
	}

	service.log.Info().Str("remote", remoteURL).Str("repo", repo).Str("reference", reference).
		Msg("sync: fetching manifest")

	if err := service.refreshRegistryTemporaryCredentials(); err != nil {
		service.log.Error().Err(err).Msg("failed to refresh credentials")
	}

	artifactRef, err := service.remote.GetImageReference(remoteRepo, reference)
	if err != nil {
		return nil, nil, err
	}

	// An upstream check is due, but "due" does not mean the image changed. One HEAD
	// answers that question; when it says the local copy is current, the client is
	// served from storage and neither this manifest nor its children are fetched.
	if service.localManifestIsUpToDate(ctx, repo, reference, artifactRef) {
		service.log.Debug().Str("remote", remoteURL).Str("repo", repo).Str("reference", reference).
			Msg("sync: local manifest matches upstream, serving it from storage")
		service.markUpstreamChecked(repo, reference)

		return nil, nil, zerr.ErrSyncManifestUpToDate
	}

	fetchedManifest, err := service.rc.ManifestGet(ctx, artifactRef)
	if err != nil {
		return nil, nil, err
	}

	var childManifests []manifest.Manifest

	// For a manifest list, each individual manifest inside it also needs
	// to be downloaded.
	if fetchedManifest.IsList() {
		indexer, ok := fetchedManifest.(manifest.Indexer)
		if !ok {
			service.log.Error().Str("remote", remoteURL).Str("repo", repo).Str("reference", reference).
				Msg("failed to cast manifest to index")

			return nil, nil, zerr.ErrBadManifest
		}

		childDescriptors, err := indexer.GetManifestList()
		if err != nil {
			service.log.Error().Err(err).Str("remote", remoteURL).Str("repo", repo).
				Str("reference", reference).Msg("failed to get manifest list")

			return nil, nil, zerr.ErrBadManifest
		}

		childManifests, err = service.fetchChildManifests(ctx, remoteRepo, repo, reference, childDescriptors)
		if err != nil {
			return nil, nil, err
		}
	}

	return fetchedManifest, childManifests, nil
}

// fetchChildManifests pulls every child of an index concurrently. The children are
// independent documents on the same host, so fetching them one by one turned a
// multi-platform image into one upstream round trip per platform - the dominant
// cost of an on-demand manifest request. Concurrency is capped by ReqConcurrent,
// the same budget regclient enforces per host, so this queues requests instead of
// exceeding the connection limit. Results keep the descriptor order.
func (service *BaseService) fetchChildManifests(ctx context.Context, remoteRepo, repo, reference string,
	childDescriptors []descriptor.Descriptor,
) ([]manifest.Manifest, error) {
	remoteURL := service.remote.GetHostName()
	childManifests := make([]manifest.Manifest, len(childDescriptors))
	semaphore := make(chan struct{}, service.childFetchConcurrency())

	// The first failure cancels the rest: the caller discards a partial index anyway.
	fetchCtx, cancelFetch := context.WithCancel(ctx)
	defer cancelFetch()

	var (
		waitGroup sync.WaitGroup
		errOnce   sync.Once
		fetchErr  error
	)

	for i, childDesc := range childDescriptors {
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if fetchCtx.Err() != nil {
				return
			}

			childManifest, err := service.fetchChildManifest(fetchCtx, remoteRepo, repo, reference,
				remoteURL, childDesc.Digest)
			if err != nil {
				errOnce.Do(func() {
					fetchErr = err

					cancelFetch()
				})

				return
			}

			childManifests[i] = childManifest
		}()
	}

	waitGroup.Wait()

	if fetchErr != nil {
		return nil, fetchErr
	}

	return childManifests, nil
}

// fetchChildManifest pulls a single child manifest of an index.
func (service *BaseService) fetchChildManifest(ctx context.Context, remoteRepo, repo, reference, remoteURL string,
	childDigest godigest.Digest,
) (manifest.Manifest, error) {
	childRef, err := service.remote.GetImageReference(remoteRepo, childDigest.String())
	if err != nil {
		service.log.Error().Err(err).Str("remote", remoteURL).Str("repo", repo).
			Str("reference", reference).Str("childDigest", childDigest.String()).
			Msg("failed to get image reference for child manifest")

		return nil, err
	}

	childManifest, err := service.rc.ManifestGet(ctx, childRef)
	if err != nil {
		service.log.Error().Err(err).Str("remote", remoteURL).Str("repo", repo).
			Str("reference", reference).Str("childDigest", childDigest.String()).
			Msg("failed to fetch child manifest")

		return nil, err
	}

	return childManifest, nil
}

// childFetchConcurrency returns how many child manifests may be fetched at once.
// It mirrors the configured per-host request budget, falling back to regclient's
// own default when ReqConcurrent is unset.
func (service *BaseService) childFetchConcurrency() int {
	if service.config.ReqConcurrent != nil && *service.config.ReqConcurrent > 0 {
		return *service.config.ReqConcurrent
	}

	return defaultChildFetchConcurrency
}
