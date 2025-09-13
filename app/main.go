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
//r умеет подключается к Kafka и читает сообщения из указанного топика
        Brokers: []string{os.Getenv("KAFKA_BROKER")},
//Сюда передаётся список адресов брокеров Kafka (например "kafka:9092"). Значение берётся из переменных окружения
        Topic:   os.Getenv("KAFKA_TOPIC"),
//Название топика
        GroupID: "orders-group",
//Идентификатор группы потребителей (consumer group)
    })
    defer r.Close()
//defer откладывает выполнение r.Close() до тех пор, пока функция consumeKafka не завершится
    for {
//Начало бесконечного цикла. Consumer постоянно ждёт новые сообщения и обрабатывает их по мере поступления
        m, err := r.ReadMessage(ctx)
//ReadMessage — блокирующий вызов: он ждёт и возвращает одно сообщение из Kafka (или ошибку). Результат: m — структура сообщения (обычно содержит 
//Topic, Partition, Offset, Key и Value). m.Value — это содержимое сообщения в виде []byte (байтовый срез), в который у тебя попадает JSON заказа.
//Вызов принимает ctx, поэтому если ctx будет отменён, ReadMessage вернёт ошибку — это механизм остановки чтения.
        if err != nil {
            log.Println("kafka read:", err)
            continue
//Если ReadMessage вернул ошибку (err != nil), мы её логируем и продолжаем цикл (continue), чтобы попытаться прочитать следующее сообщение
        }
        var o Order
//Объявляем переменную o типа Order
        if err := json.Unmarshal(m.Value, &o); err != nil || o.OrderUID == "" {
//Парсим JSON (байты m.Value) в Go-структуру o
            log.Println("bad message")
            continue
//Переходим к следующей итерации цикла — пропускаем последующие шаги для этого (плохого) сообщения
        }
        // сохраняем в БД
        _, err = db.Exec(ctx, `INSERT INTO orders (order_uid, order_json) VALUES ($1,$2)
            ON CONFLICT (order_uid) DO UPDATE SET order_json=$2`, o.OrderUID, string(m.Value))
//db.Exec возвращает (результат, error). Мы игнорируем результат и проверяем только err
//Выполняется SQL-запрос через пул db (это *pgxpool.Pool), который вставляет запись в таблицу orders
//INSERT INTO orders (order_uid, order_json) VALUES ($1,$2) — вставка нового ряда, где $1 = o.OrderUID, $2 = string(m.Value) (то есть JSON в виде строки).
//ON CONFLICT (order_uid) DO UPDATE SET order_json=$2 — если уже существует запись с таким order_uid (PK), то обновляем поле order_json новым JSONом (UPSERT).
//string(m.Value) преобразует []byte в string
        if err != nil {
            log.Println("db:", err)
            continue
////Переходим к следующей итерации цикла — пропускаем последующие шаги для этого (плохого) сообщения
        }
        // кешируем
        mu.Lock()
        cache[o.OrderUID] = o
//Записываем в кеш: по ключу order_uid сохраняем значение o (структуру Order)
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

