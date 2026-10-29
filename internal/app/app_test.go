package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"recogo/internal/reco"
	"sync"
	"testing"
	"time"
)

func setup(t *testing.T) (*Store, *Service) {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	service, err := New(s, 1)
	if err != nil {
		t.Fatal(err)
	}
	return s, service
}
func event() reco.Event {
	return reco.Event{ID: "one", User: "u1", Product: "p1", Type: "view", At: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
}
func model() reco.Model { e := event(); return reco.Fit([]reco.Event{e}) }
func trainingEvents() []reco.Event {
	events := []reco.Event{}
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 15; i++ {
		for j := 0; j < 3; j++ {
			events = append(events, reco.Event{ID: fmt.Sprintf("%d-%d", i, j), User: fmt.Sprint("u", i), Product: fmt.Sprint("p", j), Type: "view", At: start.Add(time.Duration(j) * time.Hour)})
		}
	}
	for i := 0; i < 15; i++ {
		events = append(events, reco.Event{ID: fmt.Sprint("hold", i), User: fmt.Sprint("u", i), Product: "target", Type: "purchase", At: start.Add(time.Duration(100+i) * time.Hour)})
	}
	return events
}
func TestIdempotencyAndConflicts(t *testing.T) {
	store, _ := setup(t)
	e := event()
	dup, err := store.Add(e)
	if err != nil || dup {
		t.Fatal(err)
	}
	dup, err = store.Add(e)
	if err != nil || !dup {
		t.Fatal("duplicate not recognized")
	}
	e.Product = "other"
	if _, err = store.Add(e); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	n, _ := store.Count()
	if n != 1 {
		t.Fatal(n)
	}
}
func TestConcurrentDuplicates(t *testing.T) {
	store, _ := setup(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.Add(event()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	n, _ := store.Count()
	if n != 1 {
		t.Fatal(n)
	}
}
func TestPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Add(event()); err != nil {
		t.Fatal(err)
	}
	m := model()
	if err = store.Publish(m); err != nil {
		t.Fatal(err)
	}
	store.Close()
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	s, err := New(reopened, 1)
	if err != nil || s.Active.Load().ID != m.ID {
		t.Fatalf("%v", err)
	}
	n, _ := reopened.Count()
	if n != 1 {
		t.Fatal(n)
	}
}
func TestAutomaticRecovery(t *testing.T) {
	store, _ := setup(t)
	first := model()
	first.Created = time.Now().Add(-time.Hour)
	second := model()
	if err := store.Publish(first); err != nil {
		t.Fatal(err)
	}
	if err := store.Publish(second); err != nil {
		t.Fatal(err)
	}
	store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("models")).Put([]byte(second.ID), []byte(`{"model":{},"sha256":"wrong"}`))
	})
	s, err := New(store, 1)
	if err != nil {
		t.Fatal(err)
	}
	if s.Active.Load().ID != first.ID || store.ActiveID() != first.ID || s.Rollbacks.Load() != 1 {
		t.Fatal("did not recover")
	}
}
func TestRollbackAndInvalidVersion(t *testing.T) {
	store, s := setup(t)
	first := model()
	first.Created = time.Now().Add(-time.Hour)
	second := model()
	store.Publish(first)
	store.Publish(second)
	s.Active.Store(&second)
	m, err := s.Rollback("")
	if err != nil || m.ID != first.ID {
		t.Fatal(err)
	}
	if _, err = s.Rollback("missing"); err == nil {
		t.Fatal("accepted missing version")
	}
	if s.Active.Load().ID != first.ID {
		t.Fatal("active changed on failure")
	}
}
func TestTrainKeepsActiveWhenRejected(t *testing.T) {
	store, s := setup(t)
	for _, e := range trainingEvents() {
		if _, err := store.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	report, err := s.Train(context.Background())
	if err != nil || !report.Accepted {
		t.Fatalf("%+v %v", report, err)
	}
	id := s.Active.Load().ID
	s.MinUsers = 1000
	report, err = s.Train(context.Background())
	if err != nil || report.Accepted {
		t.Fatalf("%+v %v", report, err)
	}
	if s.Active.Load().ID != id {
		t.Fatal("rejected model activated")
	}
}

type brokenTracker struct{}

func (brokenTracker) Log(context.Context, reco.Model, reco.Report) error {
	return errors.New("offline")
}
func TestTrackingOutageDoesNotBreakServing(t *testing.T) {
	store, s := setup(t)
	for _, e := range trainingEvents() {
		store.Add(e)
	}
	s.Tracker = brokenTracker{}
	report, err := s.Train(context.Background())
	if err != nil || !report.Accepted || report.TrackingError != "offline" {
		t.Fatalf("%+v %v", report, err)
	}
}
func TestHTTP(t *testing.T) {
	store, s := setup(t)
	handler := s.Handler("secret")
	call := func(method, path string, body any, token string) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(method, path, bytes.NewReader(raw))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		out := httptest.NewRecorder()
		handler.ServeHTTP(out, req)
		return out
	}
	if call("GET", "/healthz", nil, "").Code != 200 || call("GET", "/readyz", nil, "").Code != 503 {
		t.Fatal("health")
	}
	if call("POST", "/admin/train", nil, "").Code != 401 {
		t.Fatal("missing auth accepted")
	}
	if call("POST", "/events", event(), "").Code != 201 || call("POST", "/events", event(), "").Code != 200 {
		t.Fatal("ingestion")
	}
	if call("GET", "/events/one", nil, "").Code != 200 {
		t.Fatal("missing durable event")
	}
	if call("GET", "/recommendations?user_id=u&limit=-2", nil, "").Code != 400 {
		t.Fatal("bad limit")
	}
	for _, e := range trainingEvents() {
		store.Add(e)
	}
	if out := call("POST", "/admin/train", map[string]any{}, "secret"); out.Code != 200 {
		t.Fatal(out.Body.String())
	}
	if call("GET", "/readyz", nil, "").Code != 200 || call("GET", "/recommendations?user_id=new", nil, "").Code != 200 {
		t.Fatal("serving")
	}
	if call("GET", "/metrics", nil, "").Code != http.StatusOK {
		t.Fatal("metrics")
	}
}
func TestHTTPRejectsTrailingJSON(t *testing.T) {
	_, s := setup(t)
	req := httptest.NewRequest("POST", "/events", bytes.NewBufferString(`{} {}`))
	out := httptest.NewRecorder()
	s.Handler("secret").ServeHTTP(out, req)
	if out.Code != 400 {
		t.Fatal(out.Code)
	}
}
