package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/balits/kave/internal/kv"
)

var (
	nodeID    string
	operation string // get | put | del
	key       string
	value     string
	rev       int64  // for get
	end       string // for get
	prefix    bool   // for get

	prevEntries bool // for delete

	leaseID     int64 // for put
	prevEntry   bool  // for put
	ignoreValue bool  // for put
	renewLease  bool  // for put
)

func init() {
	flag.StringVar(&nodeID, "nodeID", "", "ID of the node (node1 | node2 | node3)")
	flag.StringVar(&operation, "op", "", "operation (get | put | del)")
	flag.StringVar(&key, "key", "", "key in the request")
	flag.StringVar(&value, "value", "", "value in the request")

	// get
	flag.Int64Var(&rev, "rev", 0, "get.revision in the get request")
	flag.StringVar(&end, "end", "", "get.end in the get request, with this we can do range scans")
	flag.BoolVar(&prefix, "prefix", false, "get.prefix in the get request, with this we can do prefix scans")
	// delete
	flag.BoolVar(&prevEntries, "delete.prevEntries", false, "prevEntries in the delete request")

	//put
	flag.Int64Var(&leaseID, "put.leaseID", 0, "put.leaseID in the put request")
	flag.BoolVar(&prevEntry, "put.prevEntry", false, "put.prevEntry in the put request")
	flag.BoolVar(&ignoreValue, "put.ignoreValue", false, "put.ignoreValue in the put request")
	flag.BoolVar(&renewLease, "put.renewLease", false, "put.renewLease in the put request")
}

func main() {
	flag.Parse()
	if nodeID == "" {
		fmt.Println("nodeID required")
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
	if value == "" && operation == "put" {
		fmt.Println("value required")
		os.Exit(1)
	}

	var (
		method string
		url    string
		port   string
		client = http.Client{
			Timeout: time.Second * 5,
		}
		body    []byte
		payload any
	)

	switch nodeID {
	case "node1":
		port = ":8001"
	case "node2":
		port = ":8002"
	case "node3":
		port = ":8003"
	default:
		fmt.Println("invalid node id, must be one of: node1, node2, node3")
		os.Exit(1)
	}

	url = "http://" + "localhost" + port + "/v1/kv"
	switch operation {
	case "get":
		method = "GET"
		url = url + "/get"
		payload = kv.RangeCmd{
			Key:          []byte(key),
			End:          []byte(end),
			Limit:        0,
			Linearizable: false,
			Revision:     rev,
			CountOnly:    false,
			Prefix:       prefix,
		}
	case "put":
		method = "POST"
		url = url + "/put"
		payload = kv.PutCmd{
			Key:         []byte(key),
			Value:       []byte(value),
			LeaseID:     leaseID,
			PrevEntry:   prevEntry,
			IgnoreValue: ignoreValue,
			RenewLease:  renewLease,
		}
	case "del":
		method = "DELETE"
		url = url + "/del"
		payload = kv.DeleteCmd{
			Key:         []byte(key),
			PrevEntries: prevEntries,
		}
	default:
		fmt.Println("invalid operation, must be one of: get, put, del")
		os.Exit(1)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Printf("URL: %s\nRequest body: %+v\n", url, payload)

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
	fmt.Println("< Status: ", response.Status)
	var responseBody kv.Result
	if err = json.NewDecoder(response.Body).Decode(&responseBody); err != nil {
		fmt.Println("failed to parse response (probably error happened, therefore response payload wasn't kv.Result): ", err)
		os.Exit(1)
	}
	fmt.Printf("< Body: %+v\n", responseBody)
	switch operation {
	case "get":
		if responseBody.Range != nil {
			fmt.Printf("< Payload: count: %+v\n", responseBody.Range.Count)
			for i, entry := range responseBody.Range.Entries {
				fmt.Printf("< Payload: entry %d: %s\n", i, entry)
			}
			return
		}
		fmt.Println("< No range result in response")
	case "put":
		if responseBody.Put.PrevEntry != nil {
			fmt.Printf("< Payload: %+v\n", responseBody.Put.PrevEntry)
			return
		}
		fmt.Println("< No previous entry in response")
	case "del":
		fmt.Printf("< Payload: %+v\n", responseBody.Delete)
	}
}
