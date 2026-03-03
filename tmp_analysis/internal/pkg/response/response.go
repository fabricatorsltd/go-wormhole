package response

type BaseResponse[T any] struct {
	Status  int    `json:"status"`
	Message string `json:"message,omitempty"`
	Payload T      `json:"payload,omitempty"`
}

func New[T any](status int, payload T) BaseResponse[T] {
	return BaseResponse[T]{
		Status:  status,
		Payload: payload,
	}
}

func Error[T any](status int, message string) BaseResponse[T] {
	return BaseResponse[T]{
		Status:  status,
		Message: message,
	}
}
