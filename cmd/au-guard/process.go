package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"example.org/auguard/internal/pipeline"
)

func process(
	ctx context.Context,
	source pipeline.EventSource,
	handler pipeline.EventHandler,
	out io.Writer,
) error {
	encoder := json.NewEncoder(out)

	for {
		event, err := source.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("read event: %w", err)
		}

		decision, err := handler.Handle(ctx, event)
		if err != nil {
			return err
		}

		if err := encoder.Encode(decision); err != nil {
			return fmt.Errorf("write decision: %w", err)
		}
	}
}
