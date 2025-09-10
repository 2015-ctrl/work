package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
)

type Order struct {
	OrderUID string `json:"order_uid"`
	// остальные поля опустим ради простоты, они сохраняются в raw JSON
	Raw json.RawMessage `json:"-"`
}

var (
	cache  = make(map[string]Order)
	mu     sync.RWMutex
	dbFile = "orders.json" // "имитация базы"
)

// сохраняем кэш в файл
func saveToFile() {
	mu.RLock()
	defer mu.RUnlock()
	data := make(map[string]json.RawMessage)
	for id, o := range cache {
		data[id] = o.Raw
	}
	f, _ := os.Create(dbFile)
	defer f.Close()
	json.NewEncoder(f).Encode(data)
}

// грузим кэш из файла при старте
func loadFromFile() {
	f, err := os.Open(dbFile)
	if err != nil {
		log.Println("Файл с заказами не найден, начинаем с пустого кэша")
		return
	}
	defer f.Close()

	data := make(map[string]json.RawMessage)
	if err := json.NewDecoder(f).Decode(&data); err != nil {
		log.Println("Ошибка чтения orders.json:", err)
		return
	}
	for id, raw := range data {
		cache[id] = Order{OrderUID: id, Raw: raw}
	}
	log.Println("Загружено заказов из файла:", len(cache))
}

// HTTP: получить заказ по ID
func orderHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/order/"):]
	mu.RLock()
	o, ok := cache[id]
	mu.RUnlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(o.Raw)
}

// HTTP: добавить заказ (эмуляция очереди Kafka)
func addOrderHandler(w http.ResponseWriter, r *http.Request) {
	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		http.Error(w, "невалидный JSON", 400)
		return
	}
	var tmp struct {
		OrderUID string `json:"order_uid"`
	}
	if err := json.Unmarshal(raw, &tmp); err != nil || tmp.OrderUID == "" {
		http.Error(w, "нет order_uid", 400)
		return
	}
	o := Order{OrderUID: tmp.OrderUID, Raw: raw}

	mu.Lock()
	cache[o.OrderUID] = o
	mu.Unlock()
	saveToFile()

	w.WriteHeader(201)
	fmt.Fprintf(w, "Заказ %s добавлен", o.OrderUID)
}

// HTML страница (без отдельного файла)
func uiHandler(w http.ResponseWriter, r *http.Request) {
	html := `
	<!DOCTYPE html>
	<html>
	<head><meta charset="UTF-8"><title>Поиск заказа</title></head>
	<body>
	  <h1>Найти заказ</h1>
	  <input id="orderId" placeholder="Введите order_uid">
	  <button onclick="getOrder()">Искать</button>
	  <pre id="result"></pre>
	  <script>
	    async function getOrder() {
	      const id = document.getElementById("orderId").value;
	      const res = await fetch('/order/' + id);
	      if (res.ok) {
	        document.getElementById("result").textContent = JSON.stringify(await res.json(), null, 2);
	      } else {
	        document.getElementById("result").textContent = "Заказ не найден";
	      }
	    }
	  </script>
	</body>
	</html>`
	w.Write([]byte(html))
}

func main() {
	loadFromFile()

	http.HandleFunc("/", uiHandler)
	http.HandleFunc("/order/", orderHandler)
	http.HandleFunc("/add", addOrderHandler) // POST JSON заказа

	fmt.Println("Сервис запущен на :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
