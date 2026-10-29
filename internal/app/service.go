package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"recogo/internal/reco"
	"sync"
	"sync/atomic"
	"time"
)

type Tracker interface {
	Log(context.Context, reco.Model, reco.Report) error
}
type Producer interface {
	Publish(context.Context, reco.Event) error
}
type Service struct {
	Store       *Store
	Active      atomic.Pointer[reco.Model]
	work        sync.Mutex
	Tracker     Tracker
	Producer    Producer
	MinUsers    int
	TrainOK     atomic.Int64
	TrainFailed atomic.Int64
	Rollbacks   atomic.Int64
	LastTrain   atomic.Int64
	LastQuality atomic.Int64
}

func New(store *Store, minUsers int) (*Service, error) {
	m, message, err := store.Recover()
	if err != nil {
		return nil, err
	}
	s := &Service{Store: store, MinUsers: minUsers}
	s.Active.Store(m)
	if message != "" {
		s.Rollbacks.Add(1)
		slog.Warn("automatic_rollback", "reason", message)
	}
	return s, nil
}
func (s *Service) Ingest(ctx context.Context, e reco.Event) (string, error) {
	if err := e.Validate(); err != nil {
		return "", err
	}
	if s.Producer != nil {
		if err := s.Producer.Publish(ctx, e); err != nil {
			return "", err
		}
		return "queued", nil
	}
	duplicate, err := s.Store.Add(e)
	if err != nil {
		return "", err
	}
	if duplicate {
		return "duplicate", nil
	}
	return "stored", nil
}
func (s *Service) Train(ctx context.Context) (reco.Report, error) {
	if !s.work.TryLock() {
		return reco.Report{}, errors.New("another model operation is running")
	}
	defer s.work.Unlock()
	s.LastTrain.Store(time.Now().Unix())
	events, err := s.Store.Events()
	if err != nil {
		s.TrainFailed.Add(1)
		return reco.Report{}, err
	}
	m, report, err := reco.Train(events, s.Active.Load(), s.MinUsers)
	if err != nil {
		s.TrainFailed.Add(1)
		return report, err
	}
	if s.Tracker != nil {
		if err := s.Tracker.Log(ctx, m, report); err != nil {
			report.TrackingError = err.Error()
			slog.Warn("mlflow_unavailable", "error", err)
		}
	}
	if report.Accepted {
		// Read-only inference continues using the immutable old pointer until the durable commit.
		if err := s.Store.PublishReport(m, report); err != nil {
			s.TrainFailed.Add(1)
			return report, err
		}
		s.Active.Store(&m)
		s.TrainOK.Add(1)
		s.LastQuality.Store(time.Now().Unix())
	} else {
		s.TrainFailed.Add(1)
		if err := s.Store.SaveReport(report); err != nil {
			slog.Error("save_report", "error", err)
			return report, err
		}
	}
	raw, _ := json.Marshal(report)
	slog.Info("training_finished", "report", string(raw))
	return report, nil
}
func (s *Service) Rollback(id string) (reco.Model, error) {
	if !s.work.TryLock() {
		return reco.Model{}, errors.New("another model operation is running")
	}
	defer s.work.Unlock()
	current := s.Active.Load()
	if id == "" {
		if current == nil {
			return reco.Model{}, errors.New("no active model")
		}
		versions, err := s.Store.Versions()
		if err != nil {
			return reco.Model{}, err
		}
		for _, v := range versions {
			if v.Valid && v.Created.Before(current.Created) {
				id = v.ID
				break
			}
		}
		if id == "" {
			return reco.Model{}, errors.New("no previous valid model")
		}
	}
	m, err := s.Store.Activate(id)
	if err != nil {
		return m, err
	}
	s.Active.Store(&m)
	s.Rollbacks.Add(1)
	slog.Warn("manual_rollback", "version", m.ID)
	return m, nil
}
func (s *Service) Recommend(user string, n int) (string, []reco.Item, error) {
	m := s.Active.Load()
	if m == nil {
		return "", nil, errors.New("no active model: ingest events and train first")
	}
	events, err := s.Store.User(user)
	if err != nil {
		return "", nil, err
	}
	return m.ID, m.Recommend(reco.Histories(events)[user], n), nil
}
func (s *Service) Schedule(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	timer := time.NewTicker(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if _, err := s.Train(ctx); err != nil {
				slog.Error("scheduled_training_failed", "error", err)
			}
		}
	}
}
