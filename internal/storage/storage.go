package storage

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	"order-service/internal/model"
)

type Storage struct {
	mu    sync.RWMutex
	cache map[string]model.Order
	file  string
}

func NewStorage(file string) *Storage {
	s := &Storage{
		cache: make(map[string]model.Order),
		file:  file,
	}
	s.Load() //загружает заказы из файла в кэш памяти
	return s
}

// Сохранение кэша в файл
func (s *Storage) Save() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data := make(map[string]json.RawMessage)
	for id, o := range s.cache {
		data[id] = o.Raw
	}
	f, _ := os.Create(s.file)
	defer f.Close()
	json.NewEncoder(f).Encode(data)
}

// Загрузка при старте
func (s *Storage) Load() {
	f, err := os.Open(s.file) //сигнатура метода: func Open(name string) (*File, error)
	// открываем переданный файл, то есть &Storage
	if err != nil {
		log.Println("Файл не найден, начинаем с пустого кэша")
		return
	}
	defer f.Close()

	data := make(map[string]json.RawMessage)
	if err := json.NewDecoder(f).Decode(&data); err != nil { //проверяем на ошибку при декодировании нашего json
		// из f, _ := os.Open(s.file) в data. Мапа получится, как в orders с ключом b563feb7b2b84b6test
		log.Println("Ошибка чтения файла:", err)
		return
	}
	for id, raw := range data {
		s.cache[id] = model.Order{OrderUID: id, Raw: raw} //даём значение нашей Storage и мапе в файле model.go
	}
	log.Println("Загружено заказов:", len(s.cache)) //покажет длину нашей мапы, а именно при верной загрузке "1", так как ключ у мапы один
}

func (s *Storage) Add(o model.Order) {
	s.mu.Lock()
	s.cache[o.OrderUID] = o
	s.mu.Unlock()
	s.Save()
}

func (s *Storage) Get(id string) (model.Order, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.cache[id]
	return o, ok
}
