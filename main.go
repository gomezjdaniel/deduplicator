package main

import (
	"flag"
	"log"
	"os"
	"time"
)

var (
	address     string
	maxClients  int
	outputFile  string
	reportEvery time.Duration
)

func main() {
	flag.StringVar(&address, "address", ":4000", "Address the TCP server will listen in")
	flag.IntVar(&maxClients, "max-clients", 5, "Number of maximum connected clients at a given moment")
	flag.StringVar(&outputFile, "output-file", "numbers.log", "The file were deduplicated numbers will be written to")
	flag.DurationVar(&reportEvery, "report-every", time.Second*10, "Report periodically after the given time")
	flag.Parse()

	file, err := os.OpenFile(outputFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("failed to create/open/truncate `%s` file: %w", outputFile, err)
	}

	dedup, err := newDeduplicator(address, maxClients, file, reportEvery)
	if err != nil {
		log.Fatalf("failed to create deduplicator: %s", err)
	}

	err = dedup.start()
	if err != nil {
		log.Fatalf("failed to start deduplicator: %s", err)
	}
}
