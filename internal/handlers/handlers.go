package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"order-service/internal/model"
	"order-service/internal/storage"
)

type Handlers struct {
	store *storage.Storage
}

func NewHandlers(store *storage.Storage) *Handlers {
	return &Handlers{store: store}
}

func (h *Handlers) UI(w http.ResponseWriter, r *http.Request) {
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

func (h *Handlers) GetOrder(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/order/"):]
	if o, ok := h.store.Get(id); ok {
		w.Header().Set("Content-Type", "application/json")
		w.Write(o.Raw)
		return
	}
	http.NotFound(w, r)
}

func (h *Handlers) AddOrder(w http.ResponseWriter, r *http.Request) {
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
	o := model.Order{OrderUID: tmp.OrderUID, Raw: raw}
	h.store.Add(o)
	w.WriteHeader(201)
	fmt.Fprintf(w, "Заказ %s добавлен", o.OrderUID)
}
