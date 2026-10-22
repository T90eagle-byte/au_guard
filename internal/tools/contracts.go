package tools

import (
	"context"
	"encoding/json"
)

type Tool interface {
	Name() string
	Run(context.Context, json.RawMessage) (json.RawMessage, error)
}
