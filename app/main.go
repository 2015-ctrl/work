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
//Но тег json:"order_uid" говорит: "Когда работаешь с JSON, используй ключ order_uid, а не OrderUID"
}

var (
    db    *pgxpool.Pool
    cache = make(map[string]Order)
    mu    sync.RWMutex
)

func main() {
    ctx := context.Background()
//Возвращает пустой базовый контекст без дедлайна, без таймаута, без возможности отмены. Обычно его используют в самом начале программы — как "корень дерева контекстов"

    // подключение к БД
    dsn := os.Getenv("POSTGRES_DSN")
//os.Getenv — возвращает значение переменной окружения или пустую строку, если переменная не задана
    var err error
    db, err = pgxpool.New(ctx, dsn)
//Здесь происходит подключение к БД через dns.  pgxpool.New — создаёт пул соединений с PostgreSQL (пакет pgx). В db сохраняется объект пула, через который мы будем делать запросы. 
//Передаём ctx и dsn. Если dsn неверен или БД недоступна, функция вернёт ошибку в err.
    if err != nil {
        log.Fatal(err)
    }

    // восстановить кеш
    rows, _ := db.Query(ctx, `SELECT order_json FROM orders`)
//Присваиваем переменной rows объект БД, по которому произойдёт дальнейшая итерация, в роли объекта order_json. Те самые 12345 из запроса
    for rows.Next() {
//Цикл, который перебирает все строки результата. rows.Next() возвращает true, пока есть следующая строка
        var jb []byte
//Объявляешь переменную jb, куда будешь считывать JSON (в байтовом виде). В БД order_json — тип jsonb, и драйвер отдаёт его как []byte.
        rows.Scan(&jb)
//Scan копирует значения текущей строки в указанные переменные. Здесь &jb — адрес переменной, куда поместится содержимое order_json(те самые 12345).
        var o Order
//Объявляем пустую структуру o типа Order
        if json.Unmarshal(jb, &o) == nil {
//распарсить JSON(из jb) → превратить в Go-объект(структуру o). В переменной o окажется структура, заполненная из JSON: o = Order{OrderUID: "12345"}
            cache[o.OrderUID] = o
//Записываем в кеш (в памяти) по ключу o.OrderUID значение o.  cache — это глобальная map[string]Order (определена в другом месте). В этот кеш сохраняется только 
//структура Order с одним полем OrderUID. Это причина, почему позже при запросе /order/<id> мы видим только ID, а не весь заказ — потому что 
//в кеше нет полей delivery, payment и т.д.
        }
    }
    rows.Close()
//Закрывает ресурсы, связанные с rows. Освобождает соединение/память.

    // запустить consumer
    go consumeKafka(ctx)
//Запускаем функцию consumeKafka в отдельной горутине

    // http
    http.HandleFunc("/order/", getOrder)
//Регистрируем обработчик getOrder для URL-ов, начинающихся с /order/. Пример: запрос к /order/12345 попадёт в getOrder.
//getOrder — функция, которая должна вернуть данные заказа, обращаясь в кеш или в БД.
    http.Handle("/", http.FileServer(http.Dir("/static")))
//Регистрируем файловый сервер: любые остальные запросы (например, / или /index.html) будут отдавать файлы из локальной директории /static внутри контейнера/проекта.
//То есть статическая HTML-страница (форма ввода ID) лежит в этой папке.
    log.Println("listening on :8081")
//Логируем для информативности
    log.Fatal(http.ListenAndServe(":8081", nil))
//http.ListenAndServe(":8081", nil) запускает HTTP-сервер на порту 8081. Это блокирующий вызов: функция не вернётся, пока сервер жив или пока не возникнет фатальная ошибка.
//Если ListenAndServe вернёт ошибку (например, порт занят), log.Fatal выведет ошибку и завершит программу.
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

