package dto

type QueueMessage struct {
	Version string
	Event   string
	Data    interface{}
}
