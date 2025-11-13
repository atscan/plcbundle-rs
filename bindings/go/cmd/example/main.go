package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"plcbundle"
)

func main() {
	// Command-line flags
	bundleDir := flag.String("dir", ".", "Bundle directory")
	query := flag.String("query", "did", "JMESPath query")
	simple := flag.Bool("simple", true, "Use simple mode")
	bundles := flag.String("bundles", "", "Bundles to process (e.g., 1-10, leave empty for all)")
	threads := flag.Int("threads", 0, "Number of threads (0 = auto)")
	batchSize := flag.Int("batch-size", 2000, "Batch size")

	flag.Parse()

	processor, err := plcbundle.NewProcessor(
		*bundleDir,
		*query,
		*simple,
		*threads,
		*batchSize,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer processor.Close()

	// Determine which bundles to process
	var bundleList []uint32
	if *bundles == "" {
		// Process all bundles
		lastBundle, err := plcbundle.GetLastBundle(*bundleDir)
		if err != nil {
			log.Fatal(err)
		}
		bundleList = make([]uint32, lastBundle)
		for i := uint32(1); i <= lastBundle; i++ {
			bundleList[i-1] = i
		}
		fmt.Fprintf(os.Stderr, "Processing all %d bundles\n", lastBundle)
	} else {
		// Parse specified range
		lastBundle, err := plcbundle.GetLastBundle(*bundleDir)
		if err != nil {
			log.Fatal(err)
		}
		bundleList, err = plcbundle.ParseBundles(*bundles, lastBundle)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(os.Stderr, "Processing %d bundles\n", len(bundleList))
	}

	fmt.Fprintf(os.Stderr, "Query: %s (simple mode: %v)\n\n", *query, *simple)
	start := time.Now()

	// Process with callback
	stats, err := processor.Process(bundleList, func(batch string) error {
		_, err := os.Stdout.WriteString(batch)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(start)
	matchPct := 0.0
	if stats.Operations > 0 {
		matchPct = float64(stats.Matches) / float64(stats.Operations) * 100
	}

	fmt.Fprintf(os.Stderr, "\n✓ Complete\n")
	fmt.Fprintf(os.Stderr, "  Operations: %d\n", stats.Operations)
	fmt.Fprintf(os.Stderr, "  Matches:    %d (%.2f%%)\n", stats.Matches, matchPct)
	fmt.Fprintf(os.Stderr, "  Time:       %.2fs\n", elapsed.Seconds())
	fmt.Fprintf(os.Stderr, "  Throughput: %.0f ops/sec | %.1f MB/s\n",
		float64(stats.Operations)/elapsed.Seconds(),
		float64(stats.TotalBytes)/elapsed.Seconds()/1024/1024)
}
