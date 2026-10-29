package app

import (
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"recogo/internal/reco"
	"strconv"
	"strings"
)

//go:embed dashboard.html
var dashboard string

func write(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}
func fail(w http.ResponseWriter, status int, err error) {
	write(w, status, map[string]string{"error": err.Error()})
}
func decode(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("expected one JSON object")
	}
	return nil
}

func (s *Service) Handler(token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, dashboard)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		m := s.Active.Load()
		if m == nil {
			fail(w, 503, errors.New("no active model"))
			return
		}
		write(w, 200, map[string]string{"version": m.ID, "algorithm": m.Algorithm})
	})
	mux.HandleFunc("POST /events", func(w http.ResponseWriter, r *http.Request) {
		var event reco.Event
		if err := decode(w, r, &event); err != nil {
			fail(w, 400, err)
			return
		}
		if err := event.Validate(); err != nil {
			fail(w, 400, err)
			return
		}
		status, err := s.Ingest(r.Context(), event)
		if err != nil {
			code := 503
			if errors.Is(err, ErrConflict) {
				code = 409
			}
			fail(w, code, err)
			return
		}
		code := 201
		if status == "queued" {
			code = 202
		}
		if status == "duplicate" {
			code = 200
		}
		write(w, code, map[string]string{"status": status, "event_id": event.ID})
	})
	mux.HandleFunc("GET /events/{id}", func(w http.ResponseWriter, r *http.Request) {
		found, err := s.Store.Has(r.PathValue("id"))
		if err != nil {
			fail(w, 500, err)
			return
		}
		if !found {
			fail(w, 404, errors.New("event not stored yet"))
			return
		}
		write(w, 200, map[string]bool{"stored": true})
	})
	mux.HandleFunc("GET /recommendations", func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("user_id")
		if strings.TrimSpace(user) == "" || len(user) > 128 {
			fail(w, 400, errors.New("user_id required, max 128 bytes"))
			return
		}
		n := 5
		if raw := r.URL.Query().Get("limit"); raw != "" {
			v, err := strconv.Atoi(raw)
			if err != nil || v < 1 || v > 50 {
				fail(w, 400, errors.New("limit must be 1..50"))
				return
			}
			n = v
		}
		version, items, err := s.Recommend(user, n)
		if err != nil {
			fail(w, 503, err)
			return
		}
		write(w, 200, map[string]any{"user_id": user, "version": version, "recommendations": items})
	})
	mux.HandleFunc("GET /versions", func(w http.ResponseWriter, r *http.Request) {
		v, err := s.Store.Versions()
		if err != nil {
			fail(w, 500, err)
			return
		}
		write(w, 200, v)
	})
	mux.HandleFunc("GET /report", func(w http.ResponseWriter, r *http.Request) {
		v, err := s.Store.Report()
		if err != nil {
			fail(w, 500, err)
			return
		}
		write(w, 200, v)
	})
	admin := func(fn http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if token == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
				fail(w, 401, errors.New("Bearer token required"))
				return
			}
			fn(w, r)
		}
	}
	mux.HandleFunc("POST /admin/train", admin(func(w http.ResponseWriter, r *http.Request) {
		report, err := s.Train(r.Context())
		if err != nil {
			fail(w, 409, err)
			return
		}
		write(w, 200, report)
	}))
	mux.HandleFunc("POST /admin/rollback", admin(func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Version string `json:"version"`
		}
		if err := decode(w, r, &input); err != nil {
			fail(w, 400, err)
			return
		}
		m, err := s.Rollback(input.Version)
		if err != nil {
			fail(w, 409, err)
			return
		}
		write(w, 200, map[string]string{"version": m.ID, "algorithm": m.Algorithm})
	}))
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		n, err := s.Store.Count()
		if err != nil {
			fail(w, 500, err)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		ready := 0
		if s.Active.Load() != nil {
			ready = 1
		}
		fmt.Fprintf(w, "recogo_events_stored %d\nrecogo_model_ready %d\nrecogo_training_success_total %d\nrecogo_training_failure_total %d\nrecogo_rollbacks_total %d\nrecogo_last_train_timestamp_seconds %d\nrecogo_last_accepted_timestamp_seconds %d\n", n, ready, s.TrainOK.Load(), s.TrainFailed.Load(), s.Rollbacks.Load(), s.LastTrain.Load(), s.LastQuality.Load())
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	})
}
