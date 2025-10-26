package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/balits/thesis/pkg/web"
)

var (
	nodeHttpUrl string
	operation   string // get | set | del
	key         string
	value       string
)

func init() {
	flag.StringVar(&nodeHttpUrl, "url", "", "url of the node's http server (up until the port)")
	flag.StringVar(&operation, "op", "", "operation (get | set | del)")
	flag.StringVar(&key, "key", "", "key in the request")
	flag.StringVar(&value, "value", "", "value in the request")
}

func main() {
	flag.Parse()
	if nodeHttpUrl == "" {
		fmt.Println("url required")
		os.Exit(1)
	}
	if operation == "" {
		fmt.Println("operation required")
		os.Exit(1)
	}
	if key == "" {
		fmt.Println("key required")
		os.Exit(1)
	}
	if value == "" {
		fmt.Println("value required")
		os.Exit(1)
	}

	if !strings.HasPrefix(nodeHttpUrl, "http://") {
		nodeHttpUrl = "http://" + nodeHttpUrl
	}

	var (
		method string
		url    string
		client = http.Client{
			Timeout: time.Second * 5,
		}
		body []byte
	)

	if value != "" {
		value = base64.StdEncoding.EncodeToString([]byte(value))
	}

	switch operation {
	case "get":
		method = "GET"
		url = nodeHttpUrl + "/get"
		body = []byte(`{"key":"` + key + `"}`)
	case "set":
		method = "POST"
		url = nodeHttpUrl + "/set"
		body = []byte(`{"key":"` + key + `", "value":"` + value + `"}`)
	case "del":
		method = "DELETE"
		url = nodeHttpUrl + "/del"
		body = []byte(`{"key":"` + key + `"}`)
	}

	fmt.Println("Sending request body: ", string(body))

	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	response, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	var responseBody web.ResponsePayload
	if err = json.NewDecoder(response.Body).Decode(&responseBody); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("< Status: ", response.Status)
	fmt.Printf("< Body: %+v\n", responseBody)
}
