package web

import (
	"encoding/json"
	"net/http"
)

type Context struct {
	W      http.ResponseWriter
	R      *http.Request
	Status int
}

func NewContext(w http.ResponseWriter, r *http.Request) *Context {
	return &Context{
		W: w,
		R: r,
	}
}

func (ctx *Context) NotFound() {
	ctx.Error("404 Resource not found", http.StatusNotFound)
}

func (ctx *Context) MethodNotAllowed() {
	ctx.Error("Method not allowed", http.StatusMethodNotAllowed)
}

func (ctx *Context) Error(err string, statusCode int) {
	ctx.Respond(NewResponsePayloadError(err), statusCode)
}

func (ctx *Context) ErrorWithData(data any, err string, statusCode int) {
	ctx.Respond(NewResponsePayload(data, err), statusCode)
}

func (ctx *Context) Ok(data any) {
	ctx.Respond(NewResponsePayloadOK(data), http.StatusOK)
}

func (ctx *Context) Respond(data *ResponsePayload, statusCode int) {
	h := ctx.W.Header()
	h.Set("Content-Type", "application/json")
	h.Set("X-Content-Type-Options", "nosniff")
	ctx.Status = statusCode
	ctx.W.WriteHeader(statusCode)
	ctx.WriteJSON(data)
}

func (ctx *Context) WriteJSON(data *ResponsePayload) {
	if err := json.NewEncoder(ctx.W).Encode(data); err != nil {
		http.Error(ctx.W, err.Error(), http.StatusInternalServerError)
	}
}

func (ctx *Context) ReadJSON(data any) error {
	body := ctx.R.Body
	defer body.Close()
	return json.NewDecoder(body).Decode(&data)
}

type ResponsePayload struct {
	Data  any    `json:"data"`
	Error string `json:"error"`
}

func NewResponsePayloadOK(data any) *ResponsePayload {
	return &ResponsePayload{
		Data: data,
	}
}

func NewResponsePayloadError(err string) *ResponsePayload {
	return &ResponsePayload{
		Error: err,
	}
}

func NewResponsePayload(data any, err string) *ResponsePayload {
	return &ResponsePayload{
		Data:  data,
		Error: err,
	}
}
