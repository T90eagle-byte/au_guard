package providers

import "context"

type Request struct {
	Prompt string
}

type Response struct {
	Text string
}

type Model interface {
	Complete(context.Context, Request) (Response, error)
}
