package notification

import "errors"

var (
	// ErrNotFound возвращается, когда уведомление не найдено.
	ErrNotFound = errors.New("уведомление не найдено")

	// ErrInvalidType возвращается, когда указан неизвестный тип уведомления.
	ErrInvalidType = errors.New("недопустимый тип уведомления")

	// ErrInvalidChannel возвращается, когда канал доставки не распознан.
	ErrInvalidChannel = errors.New("недопустимый канал доставки")

	// ErrCreateFailed возвращается, когда создание уведомления не удалось.
	ErrCreateFailed = errors.New("не удалось создать уведомление")
)
