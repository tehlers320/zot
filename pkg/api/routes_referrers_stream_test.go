//go:build sync && scrub && metrics && search && lint && mgmt

package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gorilla/mux"
	godigest "github.com/opencontainers/go-digest"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"
	. "github.com/smartystreets/goconvey/convey"

	"zotregistry.dev/zot/v2/pkg/api"
	"zotregistry.dev/zot/v2/pkg/api/config"
	"zotregistry.dev/zot/v2/pkg/api/constants"
	extconf "zotregistry.dev/zot/v2/pkg/extensions/config"
	syncconf "zotregistry.dev/zot/v2/pkg/extensions/config/sync"
	"zotregistry.dev/zot/v2/pkg/extensions/monitoring"
	"zotregistry.dev/zot/v2/pkg/log"
	"zotregistry.dev/zot/v2/pkg/storage/local"
	storageTypes "zotregistry.dev/zot/v2/pkg/storage/types"
)

// newReferrersStreamTestRouteHandler builds a route handler over a real local
// image store (streaming sync enabled), so the referrers lookup hits actual
// on-disk state instead of a mock.
func newReferrersStreamTestRouteHandler(
	t *testing.T,
	store storageTypes.ImageStore,
	syncOnDemand api.SyncOnDemand,
) *api.RouteHandler {
	t.Helper()

	trueVal := true

	ctlr := api.NewController(config.New())
	ctlr.Router = mux.NewRouter()
	ctlr.Config.Extensions = &extconf.ExtensionConfig{
		Sync: &syncconf.Config{Enable: &trueVal},
	}
	ctlr.StoreController.DefaultStore = store
	ctlr.SyncOnDemand = syncOnDemand

	return api.NewRouteHandler(ctlr)
}

// startStreamingRepoDir creates the on-disk state of a repo that is not served
// yet: the repo directory exists (initRepo creates the blobs/ subdir before
// writing index.json, and on object storage DirExists is a prefix lookup that
// a sibling repo already satisfies), but index.json is not readable.
func startStreamingRepoDir(t *testing.T, rootDir, repo string) {
	t.Helper()

	err := os.MkdirAll(filepath.Join(rootDir, repo, ispec.ImageBlobsDir, "sha256"), 0o700)
	if err != nil {
		t.Fatalf("failed to create in-flight repo dir: %v", err)
	}
}

// TestGetReferrersDuringStreamingSync covers the hang reported for on-demand
// `stream: true` pulls: docker follows every manifest GET with an OCI referrers
// lookup, and that lookup used to 500 while the streamed image had not been
// committed to local storage yet, so the client retried the whole pull forever.
//
// Per the GetReferrers godoc, a repository that is not served answers 200 with
// an empty manifests list - the same answer common.GetReferrers gives when the
// repo dir does not exist at all.
func TestGetReferrersDuringStreamingSync(t *testing.T) {
	Convey("GetReferrers while a streaming sync is still in flight", t, func() {
		const repo = "test/streamed"

		subject := godigest.FromString("subject-manifest")

		newReferrersReq := func() *http.Request {
			req := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodGet,
				"http://example.com/v2/"+repo+"/referrers/"+subject.String(),
				http.NoBody,
			)

			return mux.SetURLVars(req, map[string]string{
				"name":   repo,
				"digest": subject.String(),
			})
		}

		rootDir := t.TempDir()
		imgStore := local.NewImageStore(rootDir, false, false, log.NewTestLogger(),
			monitoring.NewNopMetricServer(), nil, nil, nil, nil)

		syncOnDemand := &mockSyncOnDemand{
			isStreamingEnabledForRepoFn: func(_ string) bool { return true },
		}
		handler := newReferrersStreamTestRouteHandler(t, imgStore, syncOnDemand)

		Convey("answers 200 with an empty index when the repo dir exists without index.json", func() {
			startStreamingRepoDir(t, rootDir, repo)

			rec := httptest.NewRecorder()
			handler.GetReferrers(rec, newReferrersReq())

			resp := rec.Result()
			defer resp.Body.Close()

			So(resp.StatusCode, ShouldEqual, http.StatusOK)

			var index ispec.Index
			So(json.NewDecoder(resp.Body).Decode(&index), ShouldBeNil)
			So(index.Manifests, ShouldBeEmpty)
		})

		Convey("the reported client sequence: manifest served from the stream, then referrers", func() {
			manifestContent := []byte(`{"schemaVersion":2,"mediaType":"` + ispec.MediaTypeImageManifest + `"}`)
			manifestDigest := godigest.FromBytes(manifestContent)

			syncOnDemand.fetchManifestForStreamFn = func(_ context.Context, _, _ string,
			) ([]byte, godigest.Digest, string, error) {
				// The manifest is answered straight from upstream while the
				// image is still being synced to local storage.
				startStreamingRepoDir(t, rootDir, repo)

				return manifestContent, manifestDigest, ispec.MediaTypeImageManifest, nil
			}

			manifestReq := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodGet,
				"http://example.com/v2/"+repo+"/manifests/latest",
				http.NoBody,
			)
			manifestReq = mux.SetURLVars(manifestReq, map[string]string{
				"name":      repo,
				"reference": "latest",
			})

			manifestRec := httptest.NewRecorder()
			handler.GetManifest(manifestRec, manifestReq)

			manifestResp := manifestRec.Result()
			defer manifestResp.Body.Close()

			So(manifestResp.StatusCode, ShouldEqual, http.StatusOK)
			So(manifestResp.Header.Get(constants.DistContentDigestKey), ShouldEqual, manifestDigest.String())

			// docker/containerd always follows up with a referrers lookup on the
			// digest it just received; it must not fail the pull.
			referrersReq := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodGet,
				"http://example.com/v2/"+repo+"/referrers/"+manifestDigest.String(),
				http.NoBody,
			)
			referrersReq = mux.SetURLVars(referrersReq, map[string]string{
				"name":   repo,
				"digest": manifestDigest.String(),
			})

			referrersRec := httptest.NewRecorder()
			handler.GetReferrers(referrersRec, referrersReq)

			referrersResp := referrersRec.Result()
			defer referrersResp.Body.Close()

			So(referrersResp.StatusCode, ShouldEqual, http.StatusOK)

			var index ispec.Index
			So(json.NewDecoder(referrersResp.Body).Decode(&index), ShouldBeNil)
			So(index.Manifests, ShouldBeEmpty)
		})
	})
}
