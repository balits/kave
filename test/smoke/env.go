//go:build smoke

package smoke

import (
	"fmt"
	"os"
	"strconv"
)

var env struct {
	url         string
	namespace   string
	kubeconfig  string
	clusterSize int
}

func parseEnv() {
	env.url = os.Getenv("URL")
	env.namespace = os.Getenv("NAMESPACE")
	env.kubeconfig = os.Getenv("KUBECONFIG")
	clusterSize := os.Getenv("CLUSTER_SIZE")

	if env.url == "" {
		fmt.Println("url is empty")
		os.Exit(1)
	}
	if env.namespace == "" {
		fmt.Println("NAMESPACE is empty")
		os.Exit(1)
	}
	if env.kubeconfig == "" {
		fmt.Println("KUBECONFIG is empty")
		os.Exit(1)
	}
	if clusterSize == "" {
		fmt.Println("CLUSTER_SIZE is empty")
		os.Exit(1)
	}

	i, err := strconv.Atoi(clusterSize)
	if err != nil {
		fmt.Printf("failed to parse CLUSTER_SIZE: %v\n", err)
		os.Exit(1)
	}
	env.clusterSize = i
}
