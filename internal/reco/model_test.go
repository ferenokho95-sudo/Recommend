package reco

import (
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"
)

func fixture() []Event {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []Event{}
	add := func(user, product, kind string, day int) {
		events = append(events, Event{fmt.Sprint(len(events)), user, product, kind, start.Add(time.Duration(day) * 24 * time.Hour)})
	}
	for i := 0; i < 20; i++ {
		u := fmt.Sprint("u", i)
		add(u, "A", "view", 0)
		add(u, "B", "view", 1)
	}
	for i := 0; i < 4; i++ {
		u := fmt.Sprint("mentor", i)
		add(u, "A", "view", 2)
		add(u, "B", "view", 3)
		add(u, "C", "purchase", 4)
	}
	for i := 0; i < 20; i++ {
		add(fmt.Sprint("u", i), "C", "purchase", 10+i)
	}
	return events
}

func TestValidation(t *testing.T) {
	valid := Event{"id", "user", "sku", "view", time.Now().UTC()}
	cases := []struct {
		name   string
		change func(*Event)
	}{
		{"empty ID", func(e *Event) { e.ID = "" }},
		{"invalid type", func(e *Event) { e.Type = "click" }},
		{"future", func(e *Event) { e.At = time.Now().Add(time.Hour) }},
		{"zero timestamp", func(e *Event) { e.At = time.Time{} }},
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := valid
			c.change(&e)
			if e.Validate() == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}
func TestTemporalSplit(t *testing.T) {
	events := fixture()
	train, test := Split(events)
	if len(test) == 0 || !train[len(train)-1].At.Before(test[0].At) {
		t.Fatal("split leaked timestamp")
	}
	for _, e := range train {
		if e.At.After(test[0].At) {
			t.Fatal("future in training")
		}
	}
}
func TestSimilarityAndExclusion(t *testing.T) {
	m := Fit(fixture())
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	items := m.Recommend(map[string]float64{"A": 1, "B": 1}, 5)
	if len(items) != 1 || items[0].Product != "C" {
		t.Fatalf("unexpected recommendations: %+v", items)
	}
	if len(m.Recommend(map[string]float64{"A": 1, "B": 1, "C": 1}, 5)) != 0 {
		t.Fatal("seen products returned")
	}
}
func TestColdStartAndDeterminism(t *testing.T) {
	events := fixture()
	a, b := Fit(events), Fit(events)
	if !reflect.DeepEqual(a.Popular, b.Popular) || !reflect.DeepEqual(a.Similar, b.Similar) {
		t.Fatal("nondeterministic weights")
	}
	items := a.Recommend(nil, 2)
	if len(items) != 2 || items[0].Reason != "popular_fallback" {
		t.Fatal(items)
	}
}
func TestTrainAndQualityGate(t *testing.T) {
	m, r, err := Train(fixture(), nil, 3)
	if err != nil || !r.Accepted || m.Validate() != nil {
		t.Fatalf("%+v %v", r, err)
	}
	if r.Candidate.Users < 3 || r.Candidate.HitRate < r.Baseline.HitRate {
		t.Fatalf("unexpected quality %+v", r)
	}
	_, r, err = Train(fixture(), &m, 1000)
	if err != nil || r.Accepted || r.Reason != "insufficient_evaluation_users" {
		t.Fatalf("gate failed: %+v %v", r, err)
	}
	if _, _, err = Train(fixture()[:5], nil, 3); err == nil {
		t.Fatal("accepted too little data")
	}
}
func TestOnlyFreshOutcomesEvaluateChampion(t *testing.T) {
	events := fixture()
	train, test := Split(events)
	m := Fit(train)
	result := Evaluate(m, train, test, test[len(test)-1].At)
	if result.Users != 0 {
		t.Fatal("evaluated old outcomes")
	}
}
func TestModelRejectsNonFiniteValues(t *testing.T) {
	m := Fit(fixture())
	m.Popular["A"] = math.Inf(1)
	if m.Validate() == nil {
		t.Fatal("accepted infinity")
	}
}
func TestHistoryUsesLast100Events(t *testing.T) {
	events := fixture()
	for i := 0; i < 101; i++ {
		events = append(events, Event{fmt.Sprint("recent", i), "u0", "NEW", "view", time.Date(2026, 1, 1, 0, i, 0, 0, time.UTC)})
	}
	h := Histories(events)["u0"]
	if len(h) != 1 || h["NEW"] <= 0 {
		t.Fatal(h)
	}
}

func TestQualityRegressionRetainsChampion(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []Event{}
	for i := 0; i < 20; i++ {
		for _, p := range []string{"A", "B"} {
			events = append(events, Event{fmt.Sprintf("%d-%s", i, p), fmt.Sprint("u", i), p, "view", start})
		}
	}
	for i := 0; i < 40; i++ {
		events = append(events, Event{fmt.Sprint("noise", i), fmt.Sprint("noise", i), fmt.Sprint("Z", i), "view", start.Add(time.Hour)})
	}
	for i := 0; i < 20; i++ {
		events = append(events, Event{fmt.Sprint("target", i), fmt.Sprint("u", i), "T", "purchase", start.Add(24 * time.Hour)})
	}
	champion := Model{ID: "champion", Algorithm: "item_cosine", Through: start.Add(-time.Hour), Popular: map[string]float64{"T": 1}, Similar: map[string]map[string]float64{"A": {"T": 1}, "B": {"T": 1}}}
	_, report, err := Train(events, &champion, 3)
	if err != nil || report.Accepted || report.Reason != "quality_regression_champion_retained" || report.Champion == nil || report.Champion.HitRate != 1 {
		t.Fatalf("%+v %v", report, err)
	}
}
