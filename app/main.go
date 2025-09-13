package main

import (
    "context"
    "encoding/json"
    "log"
    "net/http"
    "os"
    "strings"
    "sync"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/segmentio/kafka-go"
)

// минимальная структура для заказа
type Order struct {
    OrderUID string `json:"order_uid"`
}

var (
    db    *pgxpool.Pool
    cache = make(map[string]Order)
    mu    sync.RWMutex
)

func main() {
    ctx := context.Background()

    // подключение к БД
    dsn := os.Getenv("POSTGRES_DSN")
    var err error
    db, err = pgxpool.New(ctx, dsn)
    if err != nil {
        log.Fatal(err)
    }

    // восстановить кеш
    rows, _ := db.Query(ctx, `SELECT order_json FROM orders`)
    for rows.Next() {
        var jb []byte
        rows.Scan(&jb)
        var o Order
        if json.Unmarshal(jb, &o) == nil {
            cache[o.OrderUID] = o
        }
    }
    rows.Close()

    // запустить consumer
    go consumeKafka(ctx)

    // http
    http.HandleFunc("/order/", getOrder)
    http.Handle("/", http.FileServer(http.Dir("/static")))
    log.Println("listening on :8081")
    log.Fatal(http.ListenAndServe(":8081", nil))
}

func consumeKafka(ctx context.Context) {
    r := kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{os.Getenv("KAFKA_BROKER")},
        Topic:   os.Getenv("KAFKA_TOPIC"),
        GroupID: "orders-group",
    })
    defer r.Close()
    for {
        m, err := r.ReadMessage(ctx)
        if err != nil {
            log.Println("kafka read:", err)
            continue
        }
        var o Order
        if err := json.Unmarshal(m.Value, &o); err != nil || o.OrderUID == "" {
            log.Println("bad message")
            continue
        }
        // сохраняем в БД
        _, err = db.Exec(ctx, `INSERT INTO orders (order_uid, order_json) VALUES ($1,$2)
            ON CONFLICT (order_uid) DO UPDATE SET order_json=$2`, o.OrderUID, string(m.Value))
        if err != nil {
            log.Println("db:", err)
            continue
        }
        // кешируем
        mu.Lock()
        cache[o.OrderUID] = o
        mu.Unlock()
    }
}

func getOrder(w http.ResponseWriter, r *http.Request) {
    id := strings.TrimPrefix(r.URL.Path, "/order/")
    mu.RLock()
    o, ok := cache[id]
    mu.RUnlock()
    if !ok {
        http.Error(w, "not found", 404)
        return
    }
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(o)
}

