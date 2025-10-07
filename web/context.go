package web

import (
	"encoding/json"
	"net/http"
)

type Context struct {
	W http.ResponseWriter
	R *http.Request
}

func NewContext(w http.ResponseWriter, r *http.Request) *Context {
	return &Context{
		W: w,
		R: r,
	}
}

func (ctx *Context) NotFound() {
	data := NewResponseData(nil, "", "404 Resource not found")
	ctx.Respond(data, http.StatusNotFound)
}

func (ctx *Context) MethodNotAllowed() {
	data := NewResponseData(nil, "", "Method not allowed")
	ctx.Respond(data, http.StatusMethodNotAllowed)
}

func (ctx *Context) Respond(data *ResponseData, statusCode int) {
	h := ctx.W.Header()
	h.Set("Content-Type", "application/json")
	h.Set("X-Content-Type-Options", "nosniff")
	ctx.W.WriteHeader(statusCode)
	ctx.WriteJSON(data)
}

func (ctx *Context) WriteJSON(data *ResponseData) {
	if err := json.NewEncoder(ctx.W).Encode(data); err != nil {
		http.Error(ctx.W, err.Error(), http.StatusInternalServerError)
	}
}

func (ctx *Context) ReadJSON(data any) error {
	body := ctx.R.Body
	defer body.Close()
	return json.NewDecoder(body).Decode(&data)
}

type ResponseData struct {
	Data    any
	Message string
	Error   string
}

func NewResponseData(data any, message string, error string) *ResponseData {
	return &ResponseData{
		Data:    data,
		Message: message,
		Error:   error,
	}
}
