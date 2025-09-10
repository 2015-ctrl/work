package model

import "encoding/json"

type Order struct {
	OrderUID string          `json:"order_uid"`
	Raw      json.RawMessage `json:"-"`
}
