package tracking

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"recogo/internal/reco"
	"testing"
)

func TestMLflowLifecycleAndArtifacts(t *testing.T) {
	uploads := 0
	finished := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/2.0/mlflow/experiments/get-by-name":
			json.NewEncoder(w).Encode(map[string]any{"experiment": map[string]string{"experiment_id": "1"}})
		case "/api/2.0/mlflow/runs/create":
			json.NewEncoder(w).Encode(map[string]any{"run": map[string]any{"info": map[string]string{"run_id": "run", "artifact_uri": "mlflow-artifacts:/1/run/artifacts"}}})
		case "/api/2.0/mlflow/runs/log-batch":
			w.Write([]byte(`{}`))
		case "/api/2.0/mlflow/runs/update":
			finished = true
			w.Write([]byte(`{}`))
		case "/api/2.0/mlflow-artifacts/artifacts/1/run/artifacts/model.json", "/api/2.0/mlflow-artifacts/artifacts/1/run/artifacts/report.json":
			if r.Method != "PUT" {
				t.Error("wrong upload verb")
			}
			uploads++
			w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	err := New(server.URL).Log(context.Background(), reco.Model{ID: "version", Algorithm: "item_cosine"}, reco.Report{Accepted: true})
	if err != nil || uploads != 2 || !finished {
		t.Fatalf("%v uploads=%d finished=%v", err, uploads, finished)
	}
}
func TestMLflowFailureReported(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	if New(server.URL).Log(context.Background(), reco.Model{}, reco.Report{}) == nil {
		t.Fatal("failure swallowed")
	}
}
