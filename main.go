package main

import (
	"flag"
	"fmt"
	"github.com/jonathanmalo/mempool/services"
)

func main() {
	var flush = flag.String("flush", "", "Index you want to delete")
	flag.Parse()
	if *flush != "" {
		fmt.Println("Flushing the index:", *flush)
		services.FlushIndexData(*flush)
	} else {
		fmt.Println("Streaming mempool over IPC socket")
		services.StreamMempool()
	}
}
