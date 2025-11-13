package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"plcbundle"
)

func printUsage() {
	fmt.Println("plcbundle-go - PLC Bundle Manager CLI")
	fmt.Println("\nUsage: plcbundle-go [options] <command> [command-options]")
	fmt.Println("\nGlobal Options:")
	fmt.Println("  -dir string    Path to bundles directory (default \"./bundles\")")
	fmt.Println("\nCommands:")
	fmt.Println("  load           Load and display bundle information")
	fmt.Println("  get            Get specific operations")
	fmt.Println("  did            Query operations by DID")
	fmt.Println("  batch-did      Batch resolve multiple DIDs")
	fmt.Println("  query          Query operations with a search term")
	fmt.Println("  verify         Verify bundle integrity")
	fmt.Println("  verify-chain   Verify chain continuity")
	fmt.Println("  info           Get detailed bundle information")
	fmt.Println("  stats          Display manager statistics")
	fmt.Println("  warm-up        Warm up the cache")
	fmt.Println("  rebuild-index  Rebuild the DID index")
	fmt.Println("  index-stats    Display DID index statistics")
	fmt.Println("  help           Show this help message")
	fmt.Println("\nExamples:")
	fmt.Println("  plcbundle-go -dir test_bundles load -bundle 1")
	fmt.Println("  plcbundle-go -dir test_bundles did -did did:plc:example")
	fmt.Println("  plcbundle-go -dir test_bundles query \"create\" -b 1-5")
	fmt.Println("  plcbundle-go -dir test_bundles query \"paul.bsky.social\" --stats-only")
	fmt.Println("  plcbundle-go -dir test_bundles query \"plc_operation\" -limit 100 -f plain")
	fmt.Println("  plcbundle-go -dir test_bundles verify -bundle 1")
	fmt.Println("  plcbundle-go -dir test_bundles info -bundle 1")
}

func main() {
	bundleDir := flag.String("dir", "", "Path to bundles directory (default: current directory)")
	flag.Usage = printUsage
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(0)
	}

	command := args[0]

	// Use current working directory if not specified
	actualBundleDir := *bundleDir
	if actualBundleDir == "" {
		var err error
		actualBundleDir, err = os.Getwd()
		if err != nil {
			log.Fatalf("Failed to get current directory: %v", err)
		}
	}

	// Check if bundle directory exists
	if command != "help" {
		if _, err := os.Stat(actualBundleDir); os.IsNotExist(err) {
			log.Fatalf("Bundle directory does not exist: %s", actualBundleDir)
		}
	}

	// Create bundle manager for all commands except help
	var manager *plcbundle.BundleManager
	var err error
	if command != "help" {
		manager, err = plcbundle.NewBundleManager(*bundleDir)
		if err != nil {
			log.Fatalf("Failed to create bundle manager: %v", err)
		}
		defer manager.Close()
	}

	// Execute command
	switch command {
	case "load":
		cmdLoad(manager, args[1:])
	case "get":
		cmdGet(manager, args[1:])
	case "did":
		cmdDID(manager, args[1:])
	case "batch-did":
		cmdBatchDID(manager, args[1:])
	case "query":
		cmdQuery(manager, args[1:])
	case "verify":
		cmdVerify(manager, args[1:])
	case "verify-chain":
		cmdVerifyChain(manager, args[1:])
	case "info":
		cmdInfo(manager, args[1:])
	case "stats":
		cmdStats(manager, args[1:])
	case "warm-up":
		cmdWarmUp(manager, args[1:])
	case "rebuild-index":
		cmdRebuildIndex(manager, args[1:])
	case "index-stats":
		cmdIndexStats(manager, args[1:])
	case "help":
		printUsage()
	default:
		fmt.Printf("Unknown command: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func cmdLoad(manager *plcbundle.BundleManager, args []string) {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	bundleNum := fs.Uint("bundle", 1, "Bundle number to load")
	verifyHash := fs.Bool("verify-hash", false, "Verify hash")
	decompress := fs.Bool("decompress", true, "Decompress bundle")
	cache := fs.Bool("cache", true, "Use cache")
	fs.Parse(args)

	opts := &plcbundle.LoadOptions{
		VerifyHash: *verifyHash,
		Decompress: *decompress,
		Cache:      *cache,
	}

	result, err := manager.LoadBundle(uint32(*bundleNum), opts)
	if err != nil {
		log.Fatalf("Failed to load bundle: %v", err)
	}

	fmt.Printf("✓ Successfully loaded bundle %d\n", *bundleNum)
	fmt.Printf("  Operations: %d\n", result.OperationCount)
	fmt.Printf("  Size: %d bytes\n", result.SizeBytes)
}

func cmdGet(manager *plcbundle.BundleManager, args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	bundleNum := fs.Uint("bundle", 1, "Bundle number")
	opIndex := fs.Uint("index", 0, "Operation index")
	fs.Parse(args)

	requests := []plcbundle.OperationRequest{
		{
			BundleNum:    uint32(*bundleNum),
			OperationIdx: uint(*opIndex),
		},
	}

	operations, err := manager.GetOperationsBatch(requests)
	if err != nil {
		log.Fatalf("Failed to get operations: %v", err)
	}

	if len(operations) == 0 {
		fmt.Println("No operations found")
		return
	}

	for _, op := range operations {
		fmt.Printf("\n━━━ Operation ━━━\n")
		fmt.Printf("DID:        %s\n", op.DID)
		fmt.Printf("Type:       %s\n", op.OpType)
		fmt.Printf("CID:        %s\n", op.CID)
		fmt.Printf("Nullified:  %v\n", op.Nullified)
		fmt.Printf("Created At: %s\n", op.CreatedAt)
		fmt.Printf("JSON:       %s\n", op.JSON)
	}
}

func cmdDID(manager *plcbundle.BundleManager, args []string) {
	fs := flag.NewFlagSet("did", flag.ExitOnError)
	did := fs.String("did", "", "DID to query (required)")
	fs.Parse(args)

	if *did == "" {
		log.Fatal("DID is required (use -did flag)")
	}

	operations, err := manager.GetDIDOperations(*did)
	if err != nil {
		log.Fatalf("Failed to get DID operations: %v", err)
	}

	fmt.Printf("Found %d operations for DID: %s\n\n", len(operations), *did)

	for i, op := range operations {
		fmt.Printf("Operation %d:\n", i+1)
		fmt.Printf("  Type:       %s\n", op.OpType)
		fmt.Printf("  CID:        %s\n", op.CID)
		fmt.Printf("  Nullified:  %v\n", op.Nullified)
		fmt.Printf("  Created At: %s\n", op.CreatedAt)
		fmt.Println()
	}
}

func cmdBatchDID(manager *plcbundle.BundleManager, args []string) {
	fs := flag.NewFlagSet("batch-did", flag.ExitOnError)
	dids := fs.String("dids", "", "Comma-separated list of DIDs")
	fs.Parse(args)

	if *dids == "" {
		log.Fatal("DIDs are required (use -dids flag)")
	}

	didList := strings.Split(*dids, ",")
	for i := range didList {
		didList[i] = strings.TrimSpace(didList[i])
	}

	results, err := manager.BatchResolveDIDs(didList)
	if err != nil {
		log.Fatalf("Failed to batch resolve DIDs: %v", err)
	}

	fmt.Printf("Resolved %d DIDs\n\n", len(results))
	for did, operations := range results {
		fmt.Printf("DID: %s (%d operations)\n", did, len(operations))
	}
}

func cmdQuery(manager *plcbundle.BundleManager, args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	bundles := fs.String("b", "", "Bundle range (e.g., \"1-10\" or empty for all)")
	format := fs.String("f", "jsonl", "Output format: jsonl, json, plain")
	limit := fs.Int("limit", 0, "Maximum results to display (0 for all)")
	statsOnly := fs.Bool("stats-only", false, "Show only statistics")
	quiet := fs.Bool("q", false, "Suppress progress output")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Query bundles with simple path expressions\n\n")
		fmt.Fprintf(os.Stderr, "Usage: plcbundle-go query [OPTIONS] <EXPRESSION>\n\n")
		fmt.Fprintf(os.Stderr, "Arguments:\n")
		fmt.Fprintf(os.Stderr, "  <EXPRESSION>  Query expression (e.g., \"did\", \"create\", \"paul.bsky.social\")\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
	}

	fs.Parse(args)

	// Expression is a positional argument
	if fs.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "Error: expression is required\n\n")
		fs.Usage()
		os.Exit(1)
	}

	expression := fs.Arg(0)

	// Parse bundle range
	var startBundle, endBundle uint32
	if *bundles != "" {
		// Parse simple range like "1-10"
		parts := []rune(*bundles)
		dashIdx := -1
		for i, r := range parts {
			if r == '-' {
				dashIdx = i
				break
			}
		}

		if dashIdx > 0 {
			start, err := strconv.ParseUint(string(parts[:dashIdx]), 10, 32)
			if err != nil {
				log.Fatalf("Invalid bundle range: %v", err)
			}
			end, err := strconv.ParseUint(string(parts[dashIdx+1:]), 10, 32)
			if err != nil {
				log.Fatalf("Invalid bundle range: %v", err)
			}
			startBundle = uint32(start)
			endBundle = uint32(end)
		} else {
			// Single bundle
			num, err := strconv.ParseUint(*bundles, 10, 32)
			if err != nil {
				log.Fatalf("Invalid bundle number: %v", err)
			}
			startBundle = uint32(num)
			endBundle = uint32(num)
		}
	}

	if !*quiet {
		if startBundle == 0 && endBundle == 0 {
			fmt.Fprintf(os.Stderr, "🔍 Querying all bundles for: %s\n\n", expression)
		} else {
			fmt.Fprintf(os.Stderr, "🔍 Querying bundles %d-%d for: %s\n\n", startBundle, endBundle, expression)
		}
	}

	operations, err := manager.Query(expression, startBundle, endBundle)
	if err != nil {
		log.Fatalf("Failed to query: %v", err)
	}

	if *statsOnly {
		fmt.Printf("Matches: %d\n", len(operations))
		return
	}

	displayCount := len(operations)
	if *limit > 0 && displayCount > *limit {
		displayCount = *limit
	}

	switch *format {
	case "jsonl":
		// Output as JSON lines (like Rust CLI default)
		for i := 0; i < displayCount; i++ {
			fmt.Println(operations[i].JSON)
		}
	case "json":
		// Output as JSON array
		fmt.Println("[")
		for i := 0; i < displayCount; i++ {
			if i > 0 {
				fmt.Println(",")
			}
			fmt.Print(operations[i].JSON)
		}
		if displayCount > 0 {
			fmt.Println()
		}
		fmt.Println("]")
	case "plain":
		// Pretty output
		for i := 0; i < displayCount; i++ {
			op := operations[i]
			fmt.Printf("DID: %s | Type: %s | CID: %s | Created: %s\n",
				op.DID, op.OpType, op.CID, op.CreatedAt)
		}
	default:
		log.Fatalf("Unknown format: %s", *format)
	}

	if !*quiet && len(operations) > displayCount {
		fmt.Fprintf(os.Stderr, "\n... and %d more results (use -limit to see more)\n", len(operations)-displayCount)
	}
}

func cmdVerify(manager *plcbundle.BundleManager, args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	bundleNum := fs.Uint("bundle", 1, "Bundle number to verify")
	checkHash := fs.Bool("check-hash", true, "Check hash")
	checkChain := fs.Bool("check-chain", false, "Check chain")
	fs.Parse(args)

	result, err := manager.VerifyBundle(uint32(*bundleNum), *checkHash, *checkChain)
	if err != nil {
		log.Fatalf("Verification failed: %v", err)
	}

	fmt.Printf("Bundle %d Verification:\n", *bundleNum)
	if result.Valid {
		fmt.Println("  Status:       ✓ VALID")
	} else {
		fmt.Println("  Status:       ✗ INVALID")
	}
	fmt.Printf("  Hash Match:      %v\n", result.HashMatch)
	fmt.Printf("  Compression OK:  %v\n", result.CompressionOk)
}

func cmdVerifyChain(manager *plcbundle.BundleManager, args []string) {
	fs := flag.NewFlagSet("verify-chain", flag.ExitOnError)
	startBundle := fs.Uint("start", 1, "Start bundle number")
	endBundle := fs.Uint("end", 10, "End bundle number")
	fs.Parse(args)

	err := manager.VerifyChain(uint32(*startBundle), uint32(*endBundle))
	if err != nil {
		log.Fatalf("Chain verification failed: %v", err)
	}

	fmt.Printf("✓ Chain is valid from bundle %d to %d\n", *startBundle, *endBundle)
}

func cmdInfo(manager *plcbundle.BundleManager, args []string) {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	bundleNum := fs.Uint("bundle", 1, "Bundle number")
	includeOps := fs.Bool("include-operations", false, "Include operations")
	includeDIDs := fs.Bool("include-dids", false, "Include DIDs")
	fs.Parse(args)

	info, err := manager.GetBundleInfo(uint32(*bundleNum), *includeOps, *includeDIDs)
	if err != nil {
		log.Fatalf("Failed to get bundle info: %v", err)
	}

	fmt.Printf("\n━━━ Bundle %d Information ━━━\n\n", info.BundleNumber)
	fmt.Printf("Operations:        %d\n", info.OperationCount)
	fmt.Printf("DIDs:              %d\n", info.DIDCount)
	fmt.Printf("Compressed Size:   %d bytes (%.2f MB)\n",
		info.CompressedSize, float64(info.CompressedSize)/1024/1024)
	fmt.Printf("Uncompressed Size: %d bytes (%.2f MB)\n",
		info.UncompressedSize, float64(info.UncompressedSize)/1024/1024)
	if info.CompressedSize > 0 {
		ratio := float64(info.UncompressedSize) / float64(info.CompressedSize)
		fmt.Printf("Compression Ratio: %.2fx\n", ratio)
	}
	fmt.Printf("Start Time:        %s\n", info.StartTime)
	fmt.Printf("End Time:          %s\n", info.EndTime)
	fmt.Printf("Hash:              %s\n", info.Hash)
}

func cmdStats(manager *plcbundle.BundleManager, args []string) {
	stats, err := manager.GetStats()
	if err != nil {
		log.Fatalf("Failed to get stats: %v", err)
	}

	fmt.Println("\n━━━ Manager Statistics ━━━\n")
	fmt.Printf("Cache Hits:            %d\n", stats.CacheHits)
	fmt.Printf("Cache Misses:          %d\n", stats.CacheMisses)
	if stats.CacheHits+stats.CacheMisses > 0 {
		hitRate := float64(stats.CacheHits) / float64(stats.CacheHits+stats.CacheMisses) * 100
		fmt.Printf("Cache Hit Rate:        %.2f%%\n", hitRate)
	}
	fmt.Printf("Bytes Read:            %d (%.2f MB)\n",
		stats.BytesRead, float64(stats.BytesRead)/1024/1024)
	fmt.Printf("Operations Processed:  %d\n", stats.OperationsProcessed)
}

func cmdWarmUp(manager *plcbundle.BundleManager, args []string) {
	fs := flag.NewFlagSet("warm-up", flag.ExitOnError)
	strategy := fs.String("strategy", "recent", "Strategy: recent, all, or range")
	startBundle := fs.Uint("start", 1, "Start bundle (for range strategy)")
	endBundle := fs.Uint("end", 10, "End bundle (for range strategy)")
	fs.Parse(args)

	var strat plcbundle.WarmUpStrategy
	switch *strategy {
	case "recent":
		strat = plcbundle.WarmUpRecent
	case "all":
		strat = plcbundle.WarmUpAll
	case "range":
		strat = plcbundle.WarmUpRange
	default:
		log.Fatalf("Invalid strategy: %s (use recent, all, or range)", *strategy)
	}

	fmt.Printf("Warming up cache with strategy: %s\n", *strategy)
	err := manager.WarmUp(strat, uint32(*startBundle), uint32(*endBundle))
	if err != nil {
		log.Fatalf("Warm up failed: %v", err)
	}

	fmt.Println("✓ Cache warmed up successfully")
}

func cmdRebuildIndex(manager *plcbundle.BundleManager, args []string) {
	fmt.Println("Rebuilding DID index...")

	stats, err := manager.RebuildDIDIndex(nil)
	if err != nil {
		log.Fatalf("Failed to rebuild index: %v", err)
	}

	fmt.Println("\n━━━ Rebuild Complete ━━━\n")
	fmt.Printf("Bundles Processed:     %d\n", stats.BundlesProcessed)
	fmt.Printf("Operations Indexed:    %d\n", stats.OperationsIndexed)
	fmt.Printf("Unique DIDs:           %d\n", stats.UniqueDIDs)
	fmt.Printf("Duration:              %d ms\n", stats.DurationMs)
}

func cmdIndexStats(manager *plcbundle.BundleManager, args []string) {
	stats, err := manager.GetDIDIndexStats()
	if err != nil {
		log.Fatalf("Failed to get index stats: %v", err)
	}

	fmt.Println("\n━━━ DID Index Statistics ━━━\n")
	fmt.Printf("Total DIDs:            %d\n", stats.TotalDIDs)
	fmt.Printf("Total Operations:      %d\n", stats.TotalOperations)
	if stats.TotalDIDs > 0 {
		avg := float64(stats.TotalOperations) / float64(stats.TotalDIDs)
		fmt.Printf("Avg Operations/DID:    %.2f\n", avg)
	}
	fmt.Printf("Index Size:            %d bytes (%.2f MB)\n",
		stats.IndexSizeBytes, float64(stats.IndexSizeBytes)/1024/1024)
}
