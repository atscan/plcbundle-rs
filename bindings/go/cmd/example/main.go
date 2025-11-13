package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"plcbundle"
)

func main() {
	// Command-line flags
	bundleDir := flag.String("dir", "./bundles", "Path to bundles directory")
	bundleNum := flag.Uint("bundle", 1, "Bundle number to load")
	showHelp := flag.Bool("h", false, "Show help")

	flag.Parse()

	if *showHelp {
		fmt.Println("plcbundle-go - PLC Bundle Manager CLI")
		fmt.Println("\nUsage:")
		flag.PrintDefaults()
		fmt.Println("\nExample:")
		fmt.Println("  plcbundle-go -dir /path/to/bundles -bundle 1")
		os.Exit(0)
	}

	// Check if bundle directory exists
	if _, err := os.Stat(*bundleDir); os.IsNotExist(err) {
		log.Printf("Bundle directory does not exist: %s", *bundleDir)
		log.Printf("Creating example to demonstrate usage without actual bundles...")

		// Just show help if bundles don't exist
		fmt.Println("\nTo use this tool, you need a directory with PLC bundles.")
		fmt.Println("Usage: plcbundle-go -dir /path/to/bundles -bundle 1")
		os.Exit(0)
	}

	// Create bundle manager
	manager, err := plcbundle.NewBundleManager(*bundleDir)
	if err != nil {
		log.Fatalf("Failed to create bundle manager: %v", err)
	}
	defer manager.Close()

	fmt.Printf("Bundle manager initialized with directory: %s\n", *bundleDir)

	// Load a bundle
	opts := &plcbundle.LoadOptions{
		VerifyHash: true,
		Decompress: true,
		Cache:      true,
	}

	result, err := manager.LoadBundle(uint32(*bundleNum), opts)
	if err != nil {
		log.Fatalf("Failed to load bundle %d: %v", *bundleNum, err)
	}

	fmt.Printf("Successfully loaded bundle %d with %d operations (%d bytes)\n",
		*bundleNum, result.OperationCount, result.SizeBytes)

	// Get stats
	stats, err := manager.GetStats()
	if err != nil {
		log.Fatalf("Failed to get stats: %v", err)
	}

	fmt.Printf("Stats: Cache hits: %d, Cache misses: %d, Operations processed: %d\n",
		stats.CacheHits, stats.CacheMisses, stats.OperationsProcessed)
}
