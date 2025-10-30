package web

type JoinBody struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

type GetBody struct {
	Key string `json:"key"`
}

type SetBody struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}
type DeleteBody struct {
	GetBody
}
