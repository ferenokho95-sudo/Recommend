package app

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
	"recogo/internal/reco"
)

var ErrConflict = errors.New("event_id already exists with different payload")

type Store struct{ db *bolt.DB }
type Envelope struct {
	Model  json.RawMessage `json:"model"`
	SHA256 string          `json:"sha256"`
}
type Version struct {
	ID        string    `json:"version"`
	Created   time.Time `json:"created_at"`
	Algorithm string    `json:"algorithm"`
	Active    bool      `json:"active"`
	Valid     bool      `json:"valid"`
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{"events", "users", "models", "meta", "reports"} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db}, nil
}
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Add(e reco.Event) (bool, error) {
	if err := e.Validate(); err != nil {
		return false, err
	}
	e.At = e.At.UTC()
	data, err := json.Marshal(e)
	if err != nil {
		return false, err
	}
	duplicate := false
	err = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("events"))
		old := b.Get([]byte(e.ID))
		if old != nil {
			if !bytes.Equal(old, data) {
				return ErrConflict
			}
			duplicate = true
			return nil
		}
		if b.Stats().KeyN >= 100000 {
			return errors.New("demo storage limit reached: 100000 events")
		}
		if err := b.Put([]byte(e.ID), data); err != nil {
			return err
		}
		user, err := tx.Bucket([]byte("users")).CreateBucketIfNotExists([]byte(e.User))
		if err != nil {
			return err
		}
		return user.Put([]byte(e.ID), data)
	})
	return duplicate, err
}

func readEvents(bucket *bolt.Bucket) ([]reco.Event, error) {
	out := make([]reco.Event, 0)
	if bucket == nil {
		return out, nil
	}
	err := bucket.ForEach(func(_, v []byte) error {
		var e reco.Event
		if err := json.Unmarshal(v, &e); err != nil {
			return err
		}
		out = append(out, e)
		return nil
	})
	return out, err
}
func (s *Store) Events() ([]reco.Event, error) {
	var out []reco.Event
	err := s.db.View(func(tx *bolt.Tx) error { var err error; out, err = readEvents(tx.Bucket([]byte("events"))); return err })
	return out, err
}
func (s *Store) User(user string) ([]reco.Event, error) {
	var out []reco.Event
	err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		out, err = readEvents(tx.Bucket([]byte("users")).Bucket([]byte(user)))
		return err
	})
	return out, err
}
func (s *Store) Count() (int, error) {
	n := 0
	err := s.db.View(func(tx *bolt.Tx) error { n = tx.Bucket([]byte("events")).Stats().KeyN; return nil })
	return n, err
}
func (s *Store) Has(id string) (bool, error) {
	found := false
	err := s.db.View(func(tx *bolt.Tx) error { found = tx.Bucket([]byte("events")).Get([]byte(id)) != nil; return nil })
	return found, err
}

func decodeModel(data []byte) (reco.Model, error) {
	var envelope Envelope
	var model reco.Model
	if err := json.Unmarshal(data, &envelope); err != nil {
		return model, err
	}
	hash := sha256.Sum256(envelope.Model)
	if hex.EncodeToString(hash[:]) != envelope.SHA256 {
		return model, errors.New("model checksum mismatch")
	}
	if err := json.Unmarshal(envelope.Model, &model); err != nil {
		return model, err
	}
	return model, model.Validate()
}
func (s *Store) LoadModel(id string) (reco.Model, error) {
	var m reco.Model
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket([]byte("models")).Get([]byte(id))
		if data == nil {
			return errors.New("model version not found")
		}
		var err error
		m, err = decodeModel(data)
		return err
	})
	return m, err
}
func (s *Store) ActiveID() string {
	var id string
	s.db.View(func(tx *bolt.Tx) error { id = string(tx.Bucket([]byte("meta")).Get([]byte("active"))); return nil })
	return id
}

func (s *Store) Publish(m reco.Model) error {
	return s.publish(m, nil)
}
func (s *Store) PublishReport(m reco.Model, report reco.Report) error {
	return s.publish(m, &report)
}
func (s *Store) publish(m reco.Model, report *reco.Report) error {
	if err := m.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(raw)
	data, err := json.Marshal(Envelope{raw, hex.EncodeToString(hash[:])})
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("models"))
		if b.Get([]byte(m.ID)) != nil {
			return errors.New("model version exists")
		}
		if err := b.Put([]byte(m.ID), data); err != nil {
			return err
		}
		if report != nil {
			raw, err := json.Marshal(report)
			if err != nil {
				return err
			}
			if err := tx.Bucket([]byte("reports")).Put([]byte("latest"), raw); err != nil {
				return err
			}
		}
		return tx.Bucket([]byte("meta")).Put([]byte("active"), []byte(m.ID))
	})
}
func (s *Store) Activate(id string) (reco.Model, error) {
	var m reco.Model
	err := s.db.Update(func(tx *bolt.Tx) error {
		data := tx.Bucket([]byte("models")).Get([]byte(id))
		if data == nil {
			return errors.New("model version not found")
		}
		var err error
		m, err = decodeModel(data)
		if err != nil {
			return err
		}
		return tx.Bucket([]byte("meta")).Put([]byte("active"), []byte(id))
	})
	return m, err
}
func (s *Store) Versions() ([]Version, error) {
	out := make([]Version, 0)
	active := s.ActiveID()
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("models")).ForEach(func(k, v []byte) error {
			m, err := decodeModel(v)
			out = append(out, Version{string(k), m.Created, m.Algorithm, string(k) == active, err == nil})
			return nil
		})
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Created.After(out[j].Created) })
	return out, err
}
func (s *Store) Recover() (*reco.Model, string, error) {
	id := s.ActiveID()
	if id == "" {
		return nil, "", nil
	}
	if m, err := s.LoadModel(id); err == nil {
		return &m, "", nil
	}
	versions, err := s.Versions()
	if err != nil {
		return nil, "", err
	}
	for _, version := range versions {
		if version.ID != id && version.Valid {
			m, err := s.Activate(version.ID)
			if err == nil {
				return &m, fmt.Sprintf("invalid active version %s; reverted to %s", id, m.ID), nil
			}
		}
	}
	return nil, "", errors.New("active model is invalid and no valid fallback exists")
}
func (s *Store) SaveReport(report reco.Report) error {
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("reports")).Put([]byte("latest"), raw) })
}
func (s *Store) Report() (json.RawMessage, error) {
	out := json.RawMessage(`{}`)
	err := s.db.View(func(tx *bolt.Tx) error {
		if raw := tx.Bucket([]byte("reports")).Get([]byte("latest")); raw != nil {
			out = append(json.RawMessage(nil), raw...)
		}
		return nil
	})
	return out, err
}
