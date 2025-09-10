package main

import (
	"fmt"
	"log"
	"net/http"

	"order-service/internal/handlers"
	"order-service/internal/storage"
)

func main() {
	store := storage.NewStorage("orders.json") //это структура с полями, которые на этом этапе в случае верной загрузки json является набором из
	//внутренних числовых полей от sync.RWMutex, мапы внутри мапы с ключами b563feb7b2b84b6test и значением среза байт и поля со значением
	// json:"orders.json
	//fmt.Println(store)
	h := handlers.NewHandlers(store) //адрес структуры с полем store
	//fmt.Println(h)

	http.HandleFunc("/", h.UI)
	//fmt.Println(h.UI)
	http.HandleFunc("/order/", h.GetOrder)
	//fmt.Println(h.GetOrder)
	http.HandleFunc("/add", h.AddOrder)
	//fmt.Println(h.AddOrder)

	fmt.Println("Сервис запущен на :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
