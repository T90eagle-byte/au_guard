package providers

import "context"

type ReplayModel struct {
	Text string
}

func (m ReplayModel) Complete(
	ctx context.Context,
	_ Request,
) (Response, error) {
	if err := ctx.Err(); err != nil {
		return Response{}, err
	}

	return Response{Text: m.Text}, nil
}

var _ Model = ReplayModel{}
