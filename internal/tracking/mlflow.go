package tracking

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"recogo/internal/reco"
	"strings"
	"time"
)

type Client struct {
	Base string
	HTTP *http.Client
}

func New(base string) *Client {
	return &Client{strings.TrimRight(base, "/"), &http.Client{Timeout: 10 * time.Second}}
}
func (c *Client) request(ctx context.Context, method, path string, body any, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	if body == nil {
		raw = nil
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Base+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("MLflow %d: %s", resp.StatusCode, b)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
func (c *Client) Log(ctx context.Context, m reco.Model, r reco.Report) error {
	var experiment struct {
		Experiment struct {
			ID string `json:"experiment_id"`
		} `json:"experiment"`
	}
	path := "/api/2.0/mlflow/experiments/get-by-name?experiment_name=recogo"
	if err := c.request(ctx, "GET", path, nil, &experiment); err != nil {
		var created struct {
			ID string `json:"experiment_id"`
		}
		if createErr := c.request(ctx, "POST", "/api/2.0/mlflow/experiments/create", map[string]string{"name": "recogo"}, &created); createErr != nil {
			return createErr
		}
		experiment.Experiment.ID = created.ID
	}
	var created struct {
		Run struct {
			Info struct {
				ID       string `json:"run_id"`
				Artifact string `json:"artifact_uri"`
			} `json:"info"`
		} `json:"run"`
	}
	err := c.request(ctx, "POST", "/api/2.0/mlflow/runs/create", map[string]any{"experiment_id": experiment.Experiment.ID, "run_name": m.ID, "start_time": time.Now().UnixMilli()}, &created)
	if err != nil {
		return err
	}
	id := created.Run.Info.ID
	stamp := time.Now().UnixMilli()
	metrics := []map[string]any{}
	for key, value := range map[string]float64{"hit_rate_at_5": r.Candidate.HitRate, "baseline_hit_rate_at_5": r.Baseline.HitRate, "evaluated_users": float64(r.Candidate.Users), "events": float64(r.Events)} {
		metrics = append(metrics, map[string]any{"key": key, "value": value, "timestamp": stamp, "step": 0})
	}
	params := []map[string]string{{"key": "algorithm", "value": m.Algorithm}, {"key": "version", "value": m.ID}, {"key": "accepted", "value": fmt.Sprint(r.Accepted)}}
	if err = c.request(ctx, "POST", "/api/2.0/mlflow/runs/log-batch", map[string]any{"run_id": id, "metrics": metrics, "params": params}, nil); err != nil {
		return err
	}
	uri := created.Run.Info.Artifact
	if !strings.HasPrefix(uri, "mlflow-artifacts:/") {
		return fmt.Errorf("expected proxied artifact URI, got %s", uri)
	}
	artifactPath := strings.TrimPrefix(uri, "mlflow-artifacts:/")
	for filename, payload := range map[string]any{"model.json": m, "report.json": r} {
		raw, _ := json.MarshalIndent(payload, "", "  ")
		parts := strings.Split(strings.Trim(artifactPath, "/")+"/"+filename, "/")
		for i, p := range parts {
			parts[i] = url.PathEscape(p)
		}
		req, err := http.NewRequestWithContext(ctx, "PUT", c.Base+"/api/2.0/mlflow-artifacts/artifacts/"+strings.Join(parts, "/"), bytes.NewReader(raw))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		resp, err := c.HTTP.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			return fmt.Errorf("artifact upload HTTP %d", resp.StatusCode)
		}
	}
	return c.request(ctx, "POST", "/api/2.0/mlflow/runs/update", map[string]any{"run_id": id, "status": "FINISHED", "end_time": time.Now().UnixMilli()}, nil)
}
