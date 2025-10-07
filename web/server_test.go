package web_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/balits/thesis/web"
)

var server web.Router = newMockServer()

func TestServerGET(t *testing.T) {
	request := httptest.NewRequest("GET", "/", nil)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, request)

	t.Run("GET /", func(t *testing.T) {
		if recorder.Code != 200 {
			t.Errorf("Expected status code 200, got %d", recorder.Code)
		}

		var body web.ResponseData
		if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
			t.Errorf("Failed to decode response body: %v", err)
		}

		if body.Data != "Hello, World!" {
			t.Errorf("Expected body data 'Hello, World!', got '%v'", body.Data)
		}
	})
}

func TestServerPOST(t *testing.T) {
	name := "TestUser"
	body := []byte(`{"name":"` + name + `"}`)

	request := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	web.WithLogger(server, logger).ServeHTTP(recorder, request)

	t.Run("POST /", func(t *testing.T) {
		if recorder.Code != 200 {
			t.Errorf("Expected status code 200, got %d", recorder.Code)
		}

		var respData web.ResponseData
		if err := json.NewDecoder(recorder.Body).Decode(&respData); err != nil {
			t.Errorf("Failed to decode response body: %v", err)
		}

		if respData.Data != fmt.Sprintf("Hello, %s!", name) {
			t.Errorf("Expected body data 'Hello, %s!', got '%v'", name, respData.Data)
		}
	})
}

func newMockServer() web.Router {
	router := web.NewRouter()

	router.Register("GET", "/", func(ctx *web.Context) {
		response := web.NewResponseData("Hello, World!", "", "")
		ctx.Respond(response, 200)
	})

	router.Register("POST", "/", func(ctx *web.Context) {
		var reqBody struct {
			Name string `json:"name"`
		}
		if err := ctx.ReadJSON(&reqBody); err != nil {
			response := web.NewResponseData(nil, err.Error(), "Invalid JSON")
			ctx.Respond(response, 400)
			return
		}

		response := web.NewResponseData(fmt.Sprintf("Hello, %s!", reqBody.Name), "", "")
		ctx.Respond(response, 200)
	})

	return router
}
