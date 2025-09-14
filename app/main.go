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

//Определяем структуры заказа, тег на примере 'json:"order_uid"' говорит: "Когда работаешь с JSON, используй ключ order_uid, а не OrderUID"

type Delivery struct {
    Name    string `json:"name"`
    Phone   string `json:"phone"`
    Zip     string `json:"zip"`
    City    string `json:"city"`
    Address string `json:"address"`
    Region  string `json:"region"`
    Email   string `json:"email"`
}

type Payment struct {
    Transaction  string `json:"transaction"`
    RequestID    string `json:"request_id"`
    Currency     string `json:"currency"`
    Provider     string `json:"provider"`
    Amount       int    `json:"amount"`
    PaymentDt    int64  `json:"payment_dt"`
    Bank         string `json:"bank"`
    DeliveryCost int    `json:"delivery_cost"`
    GoodsTotal   int    `json:"goods_total"`
    CustomFee    int    `json:"custom_fee"`
}

type Item struct {
    ChrtID      int    `json:"chrt_id"`
    TrackNumber string `json:"track_number"`
    Price       int    `json:"price"`
    Rid         string `json:"rid"`
    Name        string `json:"name"`
    Sale        int    `json:"sale"`
    Size        string `json:"size"`
    TotalPrice  int    `json:"total_price"`
    NmID        int    `json:"nm_id"`
    Brand       string `json:"brand"`
    Status      int    `json:"status"`
}

type Order struct {
    OrderUID          string   `json:"order_uid"`
    TrackNumber       string   `json:"track_number"`
    Entry             string   `json:"entry"`
    Delivery          Delivery `json:"delivery"`
    Payment           Payment  `json:"payment"`
    Items             []Item   `json:"items"`
    Locale            string   `json:"locale"`
    InternalSignature string   `json:"internal_signature"`
    CustomerID        string   `json:"customer_id"`
    DeliveryService   string   `json:"delivery_service"`
    Shardkey          string   `json:"shardkey"`
    SmID              int      `json:"sm_id"`
    DateCreated       string   `json:"date_created"`
    OofShard          string   `json:"oof_shard"`
}

var (
    db    *pgxpool.Pool
//db — это глобальная переменная, в ней будет храниться подключение к базе данных PostgreSQL.  *pgxpool.Pool — это указатель на пул соединений из библиотеки pgx
//Когда наше приложение работает, оно не создаёт новое подключение к БД для каждого запроса (это очень дорого и медленно).
//Вместо этого оно берёт соединения из пула соединений (pool). Пул — это как "бассейн" с готовыми подключениями, чтобы работать быстрее
    cache = make(map[string]Order)
//В памяти создаётся карта (map) где ключ — строка (order_uid), значение — Order. Это локальный кеш заказов
    mu    sync.RWMutex
//Мьютекс с разделением на чтение/запись: позволяет безопасно читать/писать cache из нескольких горутин одновременно (RLock/RUnlock — для чтения, Lock/Unlock — для записи)
)

func main() {
    ctx := context.Background()
//Создаём базовый context, который потом передаём в операции с БД и Kafka. context.Background() — пустой корневой контекст
    dsn := os.Getenv("POSTGRES_DSN")
//Читаем переменную окружения POSTGRES_DSN (DSN — строка подключения к Postgres)

    var err error
//Объявляем переменную err заранее (используется далее для проверки ошибок)
    db, err = pgxpool.New(ctx, dsn)
//Создаём пул подключений к Postgres (pgxpool.New)
    if err != nil {
        log.Fatal(err)
//Если произошла ошибка — log.Fatal(err) выведет ошибку и завершит программу
    }

    // восстановить кеш из БД
    rows, _ := db.Query(ctx, `SELECT order_json FROM orders`)
//Выполняется запрос к БД, выбираем поле order_json из таблицы orders. Функция db.Query возвращает (rows, err) — здесь ошибка игнорируется с помощью _
    for rows.Next() {
//Перебираем все строки результата запроса
        var jb []byte
//Переменная для хранения JSON-байтов из поля order_json
        rows.Scan(&jb)
//Scan копирует значения текущей строки в указанные переменные. Здесь &jb — адрес переменной, куда поместится содержимое order_json
        var o Order
//Объявляем пустую структуру o типа Order
        if json.Unmarshal(jb, &o) == nil {
//Распарсить JSON(из jb) → превратить в Go-объект(структуру o). В переменной o окажется структура, заполненная из JSON
            cache[o.OrderUID] = o
//Записываем в кеш (в памяти) по ключу o.OrderUID значение o
        }
    }
    rows.Close()
//Закрывает ресурсы, связанные с rows. Освобождает соединение/память

    go consumeKafka(ctx)
//Запускаем функцию consumeKafka в отдельной горутине

    http.HandleFunc("/order/", getOrder)
//Регистрируем HTTP-обработчик: все запросы, начинающиеся с /order/, будут обрабатываться функцией getOrder
    http.Handle("/", http.FileServer(http.Dir("/static")))
//Регистрируем файловый сервер для корня /. Он будет отдавать файлы из директории /static. То есть, если запрос к /index.html, сервер вернёт /static/index.html
    log.Println("listening on :8081")
//Печатаем в лог сообщение, что сервер будет слушать порт 8081
    log.Fatal(http.ListenAndServe(":8081", nil))
//http.ListenAndServe(":8081", nil) запускает HTTP-сервер на порту 8081. Это блокирующий вызов: функция не вернётся, пока сервер жив или пока не возникнет фатальная ошибка.
//Если ListenAndServe вернёт ошибку (например, порт занят), log.Fatal выведет ошибку и завершит программу
}

//Функция, которая читает сообщения из Kafka и сохраняет их в БД + кеш:
func consumeKafka(ctx context.Context) {
//Объявление функции consumeKafka, принимает ctx для отмены/срока
    r := kafka.NewReader(kafka.ReaderConfig{
//r умеет подключается к Kafka и читает сообщения из указанного топика
        Brokers: []string{os.Getenv("KAFKA_BROKER")},
//Сюда передаётся список адресов брокеров Kafka. Значение берётся из переменных окружения
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
//ReadMessage — блокирующий вызов: он ждёт и возвращает одно сообщение из Kafka (или ошибку). Результат: m — структура сообщения. m.Value — это содержимое сообщения в виде []byte (байтовый срез), в который у нас попадает JSON заказа.
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
            log.Println("bad message:", string(m.Value))
            continue
//Переходим к следующей итерации цикла — пропускаем последующие шаги для этого (плохого) сообщения
        }

        _, err = db.Exec(ctx, `INSERT INTO orders (order_uid, order_json) VALUES ($1,$2)
            ON CONFLICT (order_uid) DO UPDATE SET order_json=$2`, o.OrderUID, m.Value)
//Выполняем SQL-запрос INSERT ... ON CONFLICT ...: Вставляем order_uid и order_json. Если уже есть запись с таким order_uid, обновляем поле order_json. Параметры $1 и $2 заменяются на o.OrderUID и m.Value
        if err != nil {
            log.Println("db:", err)
            continue
//Если при записи в БД возникла ошибка — логируем и пропускаем это сообщение
        }

        mu.Lock()
//Берём блокировку mu.Lock() для записи в cache
        cache[o.OrderUID] = o
//Обновляем/добавляем запись в кеш по ключу o.OrderUID
        mu.Unlock()
//mu.Unlock() снимает блокировку
    }
//Цикл продолжается — ждём следующее сообщение
}

func getOrder(w http.ResponseWriter, r *http.Request) {
//getOrder — функция-обработчик HTTP-запроса. w — куда писать ответ, r — входящий запрос
    id := strings.TrimPrefix(r.URL.Path, "/order/")
//Извлекаем id заказа из пути запроса. Если путь /order/abc123, то id станет abc123 (убираем префикс /order/)

    mu.RLock()
//Блокируем cache на чтение (RLock позволяет многим горутинам читать одновременно)
    o, ok := cache[id]
//Пробуем получить o (значение) и ok (флаг наличия) из cache
    mu.RUnlock()
//Затем снимаем блокировку RUnlock()

    if !ok {
//Если заказа нет в кеше (!ok), пытаемся взять его из базы
        var jb []byte
        err := db.QueryRow(context.Background(),
            "SELECT order_json FROM orders WHERE order_uid=$1", id).Scan(&jb)
//db.QueryRow(...).Scan(&jb) — выполняет SQL-запрос и считывает поле order_json в jb
        if err != nil {
            http.Error(w, "not found", 404)
            return
//Если QueryRow вернул ошибку (например, запись не найдена), отправляем клиенту HTTP-ошибку 404 (not found) и выходим
        }
        if json.Unmarshal(jb, &o) != nil {
//Пытаемся распарсить jb в структуру o
            http.Error(w, "bad data", 500)
            return
//Если JSON в БД испорчен — возвращаем 500 (внутренняя ошибка сервера)
        }
        mu.Lock()
        cache[id] = o
        mu.Unlock()
//Если успешно получили объект из БД — сохраняем его в кеш (под защитой блокировки mu.Lock()/Unlock()), чтобы в следующий раз не обращаться в БД
    }

    w.Header().Set("Content-Type", "application/json")
//Устанавливаем заголовок ответа, что тело будет в формате application/json
    if err := json.NewEncoder(w).Encode(o); err != nil {
//Кодируем структуру o в JSON и записываем в ответ w
        http.Error(w, "encode error", 500)
//Если при кодировании произошла ошибка — возвращаем 500 и сообщение "encode error"
    }
}

