package main

import (
	"os"
)

func main() {
	switch os.Args[1] {
	case "run":
		generateWorkload()
	case "check":
		checkWorkload()
	}
}
