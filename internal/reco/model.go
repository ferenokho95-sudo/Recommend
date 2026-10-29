package reco

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type Event struct {
	ID      string    `json:"event_id"`
	User    string    `json:"user_id"`
	Product string    `json:"product_id"`
	Type    string    `json:"type"`
	At      time.Time `json:"timestamp"`
}

func (e Event) Validate() error {
	for _, s := range []string{e.ID, e.User, e.Product} {
		if strings.TrimSpace(s) == "" || len(s) > 128 {
			return errors.New("identifiers must contain 1..128 bytes")
		}
	}
	if e.Type != "view" && e.Type != "purchase" {
		return errors.New("type must be view or purchase")
	}
	if e.At.IsZero() || e.At.After(time.Now().Add(5*time.Minute)) {
		return errors.New("invalid or future timestamp")
	}
	return nil
}

func ID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

type Item struct {
	Product string  `json:"product_id"`
	Score   float64 `json:"score"`
	Reason  string  `json:"reason"`
}
type Model struct {
	ID        string                        `json:"version"`
	Algorithm string                        `json:"algorithm"`
	Created   time.Time                     `json:"created_at"`
	Through   time.Time                     `json:"trained_through"`
	Popular   map[string]float64            `json:"popularity"`
	Similar   map[string]map[string]float64 `json:"similarities"`
}

func Histories(events []Event) map[string]map[string]float64 {
	// Last 100 events per user bound pairwise work; sort a copy for deterministic replay.
	sorted := append([]Event(nil), events...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].At.Equal(sorted[j].At) {
			return sorted[i].ID < sorted[j].ID
		}
		return sorted[i].At.Before(sorted[j].At)
	})
	groups := map[string][]Event{}
	for _, e := range sorted {
		groups[e.User] = append(groups[e.User], e)
	}
	users := map[string]map[string]float64{}
	for user, list := range groups {
		if len(list) > 100 {
			list = list[len(list)-100:]
		}
		history := map[string]float64{}
		for _, e := range list {
			weight := 1.0
			if e.Type == "purchase" {
				weight = 4
			}
			history[e.Product] += weight
		}
		for p, w := range history {
			history[p] = math.Log1p(w)
		}
		users[user] = history
	}
	return users
}

func Fit(events []Event) Model {
	m := Model{ID: ID(), Algorithm: "item_cosine", Created: time.Now().UTC(), Popular: map[string]float64{}, Similar: map[string]map[string]float64{}}
	for _, e := range events {
		if e.At.After(m.Through) {
			m.Through = e.At
		}
		w := 1.0
		if e.Type == "purchase" {
			w = 4
		}
		m.Popular[e.Product] += w
	}
	norms := map[string]float64{}
	users := Histories(events)
	// Stable addition order avoids changes due to Go map iteration order.
	names := keys(users)
	for _, u := range names {
		h := users[u]
		products := keys(h)
		for _, p := range products {
			a := h[p]
			norms[p] += a * a
			if m.Similar[p] == nil {
				m.Similar[p] = map[string]float64{}
			}
			for _, q := range products {
				if p != q {
					m.Similar[p][q] += a * h[q]
				}
			}
		}
	}
	for p, neighbors := range m.Similar {
		for q, dot := range neighbors {
			neighbors[q] = dot / math.Sqrt(norms[p]*norms[q])
		}
		// Keep at most 50 neighbors per product to bound inference and artifact size.
		ranked := rank(neighbors, nil, 50, "similar")
		top := map[string]float64{}
		for _, r := range ranked {
			top[r.Product] = r.Score
		}
		m.Similar[p] = top
	}
	return m
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func rank(scores map[string]float64, seen map[string]float64, n int, reason string) []Item {
	result := make([]Item, 0)
	for p, s := range scores {
		if _, ok := seen[p]; !ok && s > 0 {
			result = append(result, Item{p, s, reason})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Score == result[j].Score {
			return result[i].Product < result[j].Product
		}
		return result[i].Score > result[j].Score
	})
	if len(result) > n {
		result = result[:n]
	}
	return result
}

func (m Model) Recommend(history map[string]float64, n int) []Item {
	scores := map[string]float64{}
	if m.Algorithm != "popularity" {
		for _, p := range keys(history) {
			for q, similarity := range m.Similar[p] {
				scores[q] += history[p] * similarity
			}
		}
	}
	result := rank(scores, history, n, "similar_to_history")
	excluded := map[string]float64{}
	for p, w := range history {
		excluded[p] = w
	}
	for _, r := range result {
		excluded[r.Product] = 1
	}
	if len(result) < n {
		result = append(result, rank(m.Popular, excluded, n-len(result), "popular_fallback")...)
	}
	return result
}

func (m Model) Validate() error {
	if m.ID == "" || len(m.Popular) == 0 || (m.Algorithm != "item_cosine" && m.Algorithm != "popularity") {
		return errors.New("invalid model metadata")
	}
	for p, s := range m.Popular {
		if p == "" || s < 0 || math.IsNaN(s) || math.IsInf(s, 0) {
			return errors.New("invalid popularity")
		}
	}
	for _, neighbors := range m.Similar {
		for _, s := range neighbors {
			if s < 0 || s > 1.000001 || math.IsNaN(s) || math.IsInf(s, 0) {
				return errors.New("invalid similarity")
			}
		}
	}
	return nil
}

type Evaluation struct {
	Users   int     `json:"users"`
	HitRate float64 `json:"hit_rate_at_5"`
}
type Report struct {
	Version       string      `json:"version"`
	Events        int         `json:"events"`
	TrainEvents   int         `json:"train_events"`
	Candidate     Evaluation  `json:"candidate"`
	Baseline      Evaluation  `json:"baseline"`
	Champion      *Evaluation `json:"champion,omitempty"`
	Accepted      bool        `json:"accepted"`
	Reason        string      `json:"reason"`
	Created       time.Time   `json:"created_at"`
	TrackingError string      `json:"tracking_error,omitempty"`
}

func Split(events []Event) ([]Event, []Event) {
	sorted := append([]Event(nil), events...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].At.Equal(sorted[j].At) {
			return sorted[i].ID < sorted[j].ID
		}
		return sorted[i].At.Before(sorted[j].At)
	})
	if len(sorted) < 2 {
		return sorted, nil
	}
	cut := len(sorted) * 8 / 10
	// Do not split a timestamp between train and holdout.
	for cut < len(sorted) && cut > 0 && sorted[cut].At.Equal(sorted[cut-1].At) {
		cut++
	}
	return sorted[:cut], sorted[cut:]
}

func Evaluate(m Model, train, test []Event, after time.Time) Evaluation {
	histories := Histories(train)
	targets := map[string]map[string]bool{}
	for _, e := range test {
		h := histories[e.User]
		if e.Type != "purchase" || !e.At.After(after) || len(h) < 2 || h[e.Product] > 0 {
			continue
		}
		if targets[e.User] == nil {
			targets[e.User] = map[string]bool{}
		}
		targets[e.User][e.Product] = true
	}
	hits := 0
	for user, wanted := range targets {
		for _, item := range m.Recommend(histories[user], 5) {
			if wanted[item.Product] {
				hits++
				break
			}
		}
	}
	out := Evaluation{Users: len(targets)}
	if out.Users > 0 {
		out.HitRate = float64(hits) / float64(out.Users)
	}
	return out
}

func Train(events []Event, active *Model, minUsers int) (Model, Report, error) {
	if len(events) < 20 {
		return Model{}, Report{}, fmt.Errorf("need at least 20 events, got %d", len(events))
	}
	train, test := Split(events)
	if len(train) == 0 || len(test) == 0 {
		return Model{}, Report{}, errors.New("need events at different timestamps for temporal holdout")
	}
	candidate := Fit(train)
	baseline := candidate
	baseline.Algorithm = "popularity"
	c, b := Evaluate(candidate, train, test, time.Time{}), Evaluate(baseline, train, test, time.Time{})
	r := Report{Version: candidate.ID, Events: len(events), TrainEvents: len(train), Candidate: c, Baseline: b, Created: time.Now().UTC()}
	if c.Users < minUsers {
		r.Reason = "insufficient_evaluation_users"
		return candidate, r, nil
	}
	winner := "item_cosine"
	if c.HitRate < b.HitRate {
		winner = "popularity"
	}
	candidate.Algorithm = winner
	if active != nil {
		// Compare only genuinely new outcomes, after the champion's training cutoff.
		old := Evaluate(*active, train, test, active.Through)
		fresh := Evaluate(candidate, train, test, active.Through)
		if old.Users >= minUsers {
			r.Champion = &old
			if fresh.HitRate+0.05 < old.HitRate {
				r.Reason = "quality_regression_champion_retained"
				return candidate, r, nil
			}
		}
	}
	model := Fit(events)
	model.ID = candidate.ID
	model.Algorithm = winner
	if err := model.Validate(); err != nil {
		return model, r, err
	}
	r.Accepted = true
	r.Reason = "quality_gate_passed"
	return model, r, nil
}
