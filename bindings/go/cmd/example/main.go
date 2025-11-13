package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync/atomic"
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

	fmt.Fprintf(os.Stderr, "Query: %s (simple mode: %v)\n", *query, *simple)
	fmt.Fprintf(os.Stderr, "Threads: %d | Batch size: %d\n\n", *threads, *batchSize)

	// Track matches and output batches
	var matchCount atomic.Uint64
	var outputBatches atomic.Uint64
	start := time.Now()

	// Start progress updater with spinner
	done := make(chan bool)
	spinner := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	spinIdx := 0

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				matches := matchCount.Load()
				batches := outputBatches.Load()
				elapsed := time.Since(start).Seconds()

				fmt.Fprintf(os.Stderr, "\r%s Processing... | Matches: %s | Batches: %d | Time: %.1fs   ",
					spinner[spinIdx%len(spinner)],
					formatNumber(matches),
					batches,
					elapsed,
				)
				spinIdx++
			}
		}
	}()

	// Process with callback
	stats, err := processor.Process(bundleList, func(batch string) error {
		// Count lines in batch (approximate match count)
		lines := uint64(0)
		for _, c := range batch {
			if c == '\n' {
				lines++
			}
		}
		matchCount.Add(lines)
		outputBatches.Add(1)

		_, err := os.Stdout.WriteString(batch)
		return err
	})

	// Stop progress updater
	done <- true

	if err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(start)
	matchPct := 0.0
	if stats.Operations > 0 {
		matchPct = float64(stats.Matches) / float64(stats.Operations) * 100
	}

	// Clear progress line and show final stats
	fmt.Fprintf(os.Stderr, "\r%s\r", clearLine(80))
	fmt.Fprintf(os.Stderr, "✓ Complete\n")
	fmt.Fprintf(os.Stderr, "  Bundles:    %d\n", len(bundleList))
	fmt.Fprintf(os.Stderr, "  Operations: %s\n", formatNumber(stats.Operations))
	fmt.Fprintf(os.Stderr, "  Matches:    %s (%.2f%%)\n", formatNumber(stats.Matches), matchPct)
	fmt.Fprintf(os.Stderr, "  Total:      %s\n", formatBytes(stats.TotalBytes))
	fmt.Fprintf(os.Stderr, "  Matched:    %s\n", formatBytes(stats.MatchedBytes))
	fmt.Fprintf(os.Stderr, "  Time:       %.2fs\n", elapsed.Seconds())
	fmt.Fprintf(os.Stderr, "  Throughput: %s ops/sec | %s/s\n",
		formatNumber(uint64(float64(stats.Operations)/elapsed.Seconds())),
		formatBytes(uint64(float64(stats.TotalBytes)/elapsed.Seconds())),
	)
}

func clearLine(width int) string {
	line := ""
	for i := 0; i < width; i++ {
		line += " "
	}
	return line
}

func formatNumber(n uint64) string {
	s := fmt.Sprintf("%d", n)
	result := ""
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result += ","
		}
		result += string(c)
	}
	return result
}

func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
