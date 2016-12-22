package model

type Event struct {
	Ip         string
	CreatedAt  int64
	Agent      string
	Uuid       string
	Referrer   string
	Url        string
	Metric     string
	ProductId  string
	VideoId    string
	OrderId    int
	CustomerId int
	Viewer     int
	Location   string

	// Kafka encoded
	encoded []byte
	err     error
}
