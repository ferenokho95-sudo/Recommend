package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"recogo/internal/app"
	"recogo/internal/reco"
	"recogo/internal/stream"
	"recogo/internal/tracking"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := mainRun(); err != nil {
		slog.Error("command_failed", "error", err)
		os.Exit(1)
	}
}
func mainRun() error {
	if len(os.Args) < 2 {
		fmt.Println("RecoGo: serve | seed | train | rollback | sample\nSee START_HERE_RU.md")
		return nil
	}
	switch os.Args[1] {
	case "serve":
		return serve()
	case "seed", "train", "rollback", "sample", "health":
		return clientCommand(os.Args[1])
	default:
		return fmt.Errorf("unknown command %s", os.Args[1])
	}
}
func serve() error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	address := flags.String("addr", env("HTTP_ADDR", "127.0.0.1:8080"), "listen address")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	interval, err := time.ParseDuration(env("TRAIN_INTERVAL", "1h"))
	if err != nil {
		return err
	}
	minUsers, err := strconv.Atoi(env("MIN_EVAL_USERS", "3"))
	if err != nil || minUsers < 1 {
		return errors.New("MIN_EVAL_USERS must be >=1")
	}
	store, err := app.Open(filepath.Join(env("DATA_DIR", "var"), "recogo.db"))
	if err != nil {
		return err
	}
	defer store.Close()
	service, err := app.New(store, minUsers)
	if err != nil {
		return err
	}
	if uri := os.Getenv("MLFLOW_TRACKING_URI"); uri != "" {
		service.Tracker = tracking.New(uri)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	workers := make(chan error, 1)
	var consumerDone chan struct{}
	if hosts := os.Getenv("KAFKA_BROKERS"); hosts != "" {
		brokers := strings.Split(hosts, ",")
		producer := stream.NewProducer(brokers, env("KAFKA_TOPIC", "product-events"))
		defer producer.Close()
		service.Producer = producer
		consumerDone = make(chan struct{})
		go func() {
			defer close(consumerDone)
			workers <- stream.Consume(ctx, brokers, env("KAFKA_TOPIC", "product-events"), env("KAFKA_DLQ", "product-events-dlq"), store)
		}()
	}
	schedulerDone := make(chan struct{})
	go func() { defer close(schedulerDone); service.Schedule(ctx, interval) }()
	server := &http.Server{Addr: *address, Handler: service.Handler(env("ADMIN_TOKEN", "recogo-local")), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 2 * time.Minute, IdleTimeout: 60 * time.Second}
	serverErrors := make(chan error, 1)
	go func() { slog.Info("server_listening", "address", *address); serverErrors <- server.ListenAndServe() }()
	var result error
	select {
	case <-ctx.Done():
	case err := <-workers:
		result = fmt.Errorf("Kafka consumer stopped: %w", err)
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			result = err
		}
	}
	cancel()
	shutdown, done := context.WithTimeout(context.Background(), 15*time.Second)
	defer done()
	server.Shutdown(shutdown)
	<-schedulerDone
	if consumerDone != nil {
		<-consumerDone
	}
	return result
}

func send(base, token, path string, body any) (json.RawMessage, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequest("POST", strings.TrimRight(base, "/")+path, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	client := &http.Client{Timeout: 2 * time.Minute}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d: %s", response.StatusCode, data)
	}
	return data, nil
}
func demo() []reco.Event {
	end := time.Now().UTC().Add(-24 * time.Hour).Truncate(24 * time.Hour)
	start := end.Add(-10 * 24 * time.Hour)
	result := []reco.Event{}
	add := func(id, user, product, kind string, at time.Time) {
		result = append(result, reco.Event{ID: id + "-" + end.Format("20060102"), User: user, Product: product, Type: kind, At: at})
	}
	for i := 0; i < 60; i++ {
		u := fmt.Sprintf("user-%03d", i+1)
		group := i % 5
		for j := 0; j < 3; j++ {
			add(fmt.Sprintf("seed-%d-%d", i, j), u, fmt.Sprintf("SKU-%03d", group*8+j+1), "view", start.Add(time.Duration(i*3+j)*time.Minute))
		}
	}
	for i := 0; i < 10; i++ {
		group := i % 5
		u := fmt.Sprintf("mentor-%02d", i)
		for j, p := range []int{1, 2, 4, 5, 6} {
			add(fmt.Sprintf("mentor-%d-%d", i, j), u, fmt.Sprintf("SKU-%03d", group*8+p), "purchase", start.Add(24*time.Hour+time.Duration(i*5+j)*time.Minute))
		}
	}
	for i := 0; i < 60; i++ {
		add(fmt.Sprintf("outcome-%d", i), fmt.Sprintf("user-%03d", i+1), fmt.Sprintf("SKU-%03d", (i%5)*8+5), "purchase", end.Add(time.Duration(i-60)*time.Minute))
	}
	return result
}
func clientCommand(command string) error {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	base := flags.String("url", env("RECOGO_URL", "http://127.0.0.1:8080"), "API URL")
	token := flags.String("token", env("ADMIN_TOKEN", "recogo-local"), "admin token")
	version := flags.String("version", "", "rollback target, empty for previous")
	file := flags.String("file", "", "JSONL file for seed/sample")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if command == "health" {
		client := http.Client{Timeout: 3 * time.Second}
		resp, err := client.Get(strings.TrimRight(*base, "/") + "/healthz")
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return errors.New("unhealthy API")
		}
		return nil
	}
	if command == "sample" {
		path := *file
		if path == "" {
			path = "data/events.jsonl"
		}
		if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
			return err
		}
		out, err := os.Create(path)
		if err != nil {
			return err
		}
		defer out.Close()
		encoder := json.NewEncoder(out)
		for _, e := range demo() {
			if err := encoder.Encode(e); err != nil {
				return err
			}
		}
		fmt.Println(path)
		return nil
	}
	if command == "train" {
		out, err := send(*base, *token, "/admin/train", map[string]any{})
		if err != nil {
			return err
		}
		fmt.Println(string(out))
		var r reco.Report
		if err := json.Unmarshal(out, &r); err != nil {
			return err
		}
		if !r.Accepted {
			return fmt.Errorf("model rejected: %s", r.Reason)
		}
		return nil
	}
	if command == "rollback" {
		out, err := send(*base, *token, "/admin/rollback", map[string]string{"version": *version})
		if err != nil {
			return err
		}
		fmt.Println(string(out))
		return nil
	}
	events := demo()
	if *file != "" {
		source, err := os.Open(*file)
		if err != nil {
			return err
		}
		defer source.Close()
		events = nil
		decoder := json.NewDecoder(source)
		for {
			var e reco.Event
			err := decoder.Decode(&e)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			events = append(events, e)
		}
	}
	for _, e := range events {
		if _, err := send(*base, *token, "/events", e); err != nil {
			return err
		}
	}
	// A producer acknowledgement is not a consumer commit. Verify each event ID is durable.
	client := http.Client{Timeout: 3 * time.Second}
	deadline := time.Now().Add(45 * time.Second)
	for _, e := range events {
		for {
			resp, err := client.Get(strings.TrimRight(*base, "/") + "/events/" + url.PathEscape(e.ID))
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == 200 {
					break
				}
			}
			if time.Now().After(deadline) {
				return errors.New("events queued but not stored before timeout; check Kafka consumer/DLQ")
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	fmt.Printf("Stored %d events. Ready for recogo train.\n", len(events))
	return nil
}
