package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

const (
	maxConcurrentDownloads = 4
	maxRetries             = 3
	retryDelay             = 3 * time.Second
)

type rpcRequest struct {
	JsonRPC string        `json:"jsonrpc"`
	ID      string        `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}
type rpcResponse struct {
	JsonRPC string          `json:"jsonrpc"`
	ID      string          `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

var ariaProcess *exec.Cmd

func fetchURL(url string) (string, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.AddCookie(&http.Cookie{Name: "openintel-data-agreement-accepted", Value: "true"})
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func extractURLs(content string, pattern string) []string {
	re := regexp.MustCompile(pattern)
	matches := re.FindAllString(content, -1)
	return matches
}

func printHelp() {
	fmt.Println(`
█████▀███████████████████████████████████████████
█─▄▄▄▄█▄─▄▄─█─▄─▄─█▄─▄█▄─▀█▄─▄█─▄─▄─█▄─▄▄─█▄─▄███
█─██▄─██─▄█▀███─████─███─█▄▀─████─████─▄█▀██─██▀█
▀▄▄▄▄▄▀▄▄▄▄▄▀▀▄▄▄▀▀▄▄▄▀▄▄▄▀▀▄▄▀▀▄▄▄▀▀▄▄▄▄▄▀▄▄▄▄▄▀
	`)
	fmt.Println("Usage: getintel [options]")
	fmt.Println("Options:")
	fmt.Println("  -src string")
	fmt.Println("        Source for the data (options: alexa, umbrella, tranco, radar, majestic, crux)")
	fmt.Println("  -y string")
	fmt.Println("        Year for the data (default is 2025). Supports single year (2024), or range (2023-2025)")
	fmt.Println("  -m string")
	fmt.Println("        Month for the data (default is 03). Supports single month (02), or range (01-04)")
	fmt.Println("  -d string")
	fmt.Println("        Day for the data: single (e.g. 14) or range (e.g. 01-20)")
	fmt.Println("  -c string")
	fmt.Println("        Column name for parsing (default: 'response_name') OR country code for crux source (e.g. 'global', 'us', 'gb', 'ca', ...)")
	fmt.Println("  -parse")
	fmt.Println("        Parse all Parquet files in the current directory and print values from specified column (default: response_name)")
	fmt.Println("  -aria-binary string")
	fmt.Println("        Path to aria2c binary (for download mode) (default 'aria2c')")
	fmt.Println("  -rpc string")
	fmt.Println("        aria2c RPC server address (default 'http://127.0.0.1:6800/jsonrpc')")
	fmt.Println("  -token string")
	fmt.Println("        aria2c RPC token (default empty)")
	fmt.Println("  -dir string")
	fmt.Println("        Download directory (for aria2c)")
	fmt.Println("  -l")
	fmt.Println("        List all generated download URLs given -src and filters (no download)")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  getintel -src=umbrella -y=2025 -m=03 -d=14")
	fmt.Println("  getintel -src=umbrella -y=2025 -m=03 -d=01-10")
	fmt.Println("  getintel -src=crux -y=2025 -m=06 -d=27 -c=global")
	fmt.Println("  getintel -src=crux -y=2025 -m=06 -d=10-15 -c=global")
	fmt.Println("  getintel -src=tranco -y=2024-2025 -m=01-03 -d=14")
	fmt.Println("  getintel -parse")
	fmt.Println("  getintel -parse -c domain")
	os.Exit(0)
}

func parseDayRange(day string) []string {
	var days []string
	if day == "" {
		for d := 1; d <= 31; d++ {
			days = append(days, fmt.Sprintf("%02d", d))
		}
		return days
	}
	if strings.Contains(day, "-") {
		parts := strings.SplitN(day, "-", 2)
		start, err1 := strconv.Atoi(parts[0])
		end, err2 := strconv.Atoi(parts[1])
		if err1 != nil || err2 != nil || start < 1 || end > 31 || start > end {
			fmt.Fprintf(os.Stderr, "Invalid day range: %s\n", day)
			os.Exit(1)
		}
		for d := start; d <= end; d++ {
			days = append(days, fmt.Sprintf("%02d", d))
		}
		return days
	}
	// Single day
	if d, err := strconv.Atoi(day); err == nil && d >= 1 && d <= 31 {
		days = append(days, fmt.Sprintf("%02d", d))
		return days
	}
	fmt.Fprintf(os.Stderr, "Invalid day format: %s\n", day)
	os.Exit(1)
	return nil
}

func parseYearRange(yr string) []string {
	var years []string
	if yr == "" {
		years = append(years, "2025")
		return years
	}
	if strings.Contains(yr, "-") {
		parts := strings.SplitN(yr, "-", 2)
		start, err1 := strconv.Atoi(parts[0])
		end, err2 := strconv.Atoi(parts[1])
		if err1 != nil || err2 != nil || start > end {
			fmt.Fprintf(os.Stderr, "Invalid year range: %s\n", yr)
			os.Exit(1)
		}
		for y := start; y <= end; y++ {
			years = append(years, fmt.Sprintf("%04d", y))
		}
		return years
	}
	years = append(years, yr)
	return years
}

func parseMonthRange(m string) []string {
	var months []string
	if m == "" {
		for mm := 1; mm <= 12; mm++ {
			months = append(months, fmt.Sprintf("%02d", mm))
		}
		return months
	}
	if strings.Contains(m, "-") {
		parts := strings.SplitN(m, "-", 2)
		start, err1 := strconv.Atoi(parts[0])
		end, err2 := strconv.Atoi(parts[1])
		if err1 != nil || err2 != nil || start < 1 || end > 12 || start > end {
			fmt.Fprintf(os.Stderr, "Invalid month range: %s\n", m)
			os.Exit(1)
		}
		for mm := start; mm <= end; mm++ {
			months = append(months, fmt.Sprintf("%02d", mm))
		}
		return months
	}
	// Single month
	if mm, err := strconv.Atoi(m); err == nil && mm >= 1 && mm <= 12 {
		months = append(months, fmt.Sprintf("%02d", mm))
		return months
	}
	fmt.Fprintf(os.Stderr, "Invalid month format: %s\n", m)
	os.Exit(1)
	return nil
}

func parseParquetFiles(columnName string) {
	if columnName == "" {
		columnName = "response_name"
	}
	files, err := filepath.Glob("*.parquet")
	if err != nil {
		log.Fatalf("Error finding Parquet files: %v", err)
	}
	if len(files) == 0 {
		fmt.Println("No Parquet files found in current directory")
		return
	}
	totalCount := 0
	for _, f := range files {
		// fmt.Printf("Processing: %s\n", f)
		db, err := sql.Open("duckdb", "")
		if err != nil {
			log.Printf("Failed to open DuckDB for file %s: %v", f, err)
			continue
		}
		query := fmt.Sprintf(`
			SELECT DISTINCT %s
			FROM read_parquet('%s')
			WHERE %s IS NOT NULL
		`, columnName, strings.ReplaceAll(f, "'", "''"), columnName)
		rows, err := db.Query(query)
		if err != nil {
			log.Printf("Query failed for file %s: %v", f, err)
			db.Close()
			continue
		}
		var value string
		count := 0
		for rows.Next() {
			if err := rows.Scan(&value); err != nil {
				log.Printf("Error scanning row in %s: %v", f, err)
				continue
			}
			fmt.Println(value)
			totalCount++
			count++
		}
		if err := rows.Err(); err != nil {
			log.Printf("Error reading rows from %s: %v", f, err)
		}
		rows.Close()
		db.Close()
		// fmt.Printf("  -> %d unique values in %s\n", count, f)
	}
	// fmt.Printf("\nFound %d unique values in total (across all files).\n", totalCount)
}

// --- aria2/RPC, worker, and other helper functions here ---
func pollDownloadCompletion(rpcAddr, token, gid string) error {
	for {
		status, err := getDownloadStatus(rpcAddr, token, gid)
		if err != nil {
			return err
		}
		if status == "complete" || status == "error" || status == "removed" {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
}

func getDownloadStatus(rpcAddr, token, gid string) (string, error) {
	params := []interface{}{}
	if token != "" {
		params = append(params, "token:"+token)
	}
	params = append(params, gid, []string{"status"})
	req := rpcRequest{
		JsonRPC: "2.0",
		ID:      "status-" + gid,
		Method:  "aria2.tellStatus",
		Params:  params,
	}
	payload, _ := json.Marshal(req)
	resp, err := http.Post(rpcAddr, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var rpcResp rpcResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return "", err
	}
	if rpcResp.Error != nil {
		return "", errors.New(rpcResp.Error.Message)
	}
	var result struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rpcResp.Result, &result); err != nil {
		return "", err
	}
	return result.Status, nil
}

func sendAddUri(rpcAddr, token, uri, id string) (string, error) {
	params := []interface{}{}
	if token != "" {
		params = append(params, "token:"+token)
	}
	params = append(params, []string{uri})
	req := rpcRequest{
		JsonRPC: "2.0",
		ID:      id,
		Method:  "aria2.addUri",
		Params:  params,
	}
	payload, _ := json.Marshal(req)
	resp, err := http.Post(rpcAddr, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var rpcResp rpcResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return "", err
	}
	if rpcResp.Error != nil {
		return "", errors.New(rpcResp.Error.Message)
	}
	var gid string
	if err := json.Unmarshal(rpcResp.Result, &gid); err != nil {
		return "", err
	}
	return gid, nil
}

func sendShutdown(rpcAddr, token string) {
	params := []interface{}{}
	if token != "" {
		params = append(params, "token:"+token)
	}
	req := rpcRequest{
		JsonRPC: "2.0",
		ID:      "shutdown",
		Method:  "aria2.shutdown",
		Params:  params,
	}
	payload, _ := json.Marshal(req)
	http.Post(rpcAddr, "application/json", bytes.NewBuffer(payload))
}

func checkServer(rpcAddr, token string) bool {
	req := rpcRequest{
		JsonRPC: "2.0",
		ID:      "check",
		Method:  "aria2.getVersion",
		Params:  []interface{}{},
	}
	if token != "" {
		req.Params = append([]interface{}{"token:" + token}, req.Params...)
	}
	cli := &http.Client{Timeout: 1 * time.Second}
	payload, _ := json.Marshal(req)
	resp, err := cli.Post(rpcAddr, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

func waitForServer(rpcAddr, token string) {
	timeout := time.After(5 * time.Second)
	tick := time.Tick(300 * time.Millisecond)
	for {
		select {
		case <-timeout:
			log.Fatalf("Timeout waiting for aria2c RPC server")
		case <-tick:
			if checkServer(rpcAddr, token) {
				return
			}
		}
	}
}

func startAria2Server(bin, token, dir, origRPCAddr string) (string, *exec.Cmd, error) {
	serverURL, _ := url.Parse(origRPCAddr)
	port := "6800"
	if p := serverURL.Port(); p != "" {
		port = p
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return "", nil, err
	}
	args := []string{
		"--enable-rpc",
		"--rpc-listen-all=false",
		fmt.Sprintf("--rpc-listen-port=%s", port),
		"--rpc-allow-origin-all",
		"--file-allocation=none",
		"--max-concurrent-downloads=10",
		"--max-connection-per-server=4",
		fmt.Sprintf("--dir=%s", absDir),
	}
	if token != "" {
		args = append(args, fmt.Sprintf("--rpc-secret=%s", token))
	}
	cmd := exec.Command(bin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return "", nil, err
	}
	addr := "http://127.0.0.1:" + port + "/jsonrpc"
	return addr, cmd, nil
}

func shutdownServer() {
	if ariaProcess != nil {
		ariaProcess.Process.Signal(syscall.SIGTERM)
		ariaProcess.Wait()
	}
}

// worker function for the download queue, only sends new downloads when a "spot" is open
func worker(rpcAddr, rpcToken string, queue <-chan string, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	for url := range queue {
		var gid string
		var err error
		// Add only one download at a time
		for attempt := 1; attempt <= maxRetries; attempt++ {
			gid, err = sendAddUri(rpcAddr, rpcToken, url, fmt.Sprintf("dl-%d-%d", id, attempt))
			if err == nil {
				log.Printf("[worker %d] ✅ Added: %s (GID: %s)", id, url, gid)
				break
			}
			log.Printf("[worker %d] ❌ Failed to add: %s (attempt %d/%d): %v", id, url, attempt, maxRetries, err)
			time.Sleep(retryDelay)
		}
		if err != nil {
			log.Printf("[worker %d] Gave up adding: %s", id, url)
			continue
		}

		// Wait for this download to finish
		for attempt := 1; attempt <= maxRetries; attempt++ {
			perr := pollDownloadCompletion(rpcAddr, rpcToken, gid)
			if perr == nil {
				break
			}
			log.Printf("[worker %d] ❌ Error polling GID %s (attempt %d/%d): %v", id, gid, attempt, maxRetries, perr)
			time.Sleep(retryDelay)
		}
	}
}

func main() {
	source := flag.String("src", "", "Source for the data (options: alexa, umbrella, tranco, radar, majestic, crux)")
	year := flag.String("y", "2025", "Year for the data")
	month := flag.String("m", "03", "Month for the data")
	day := flag.String("d", "", "Day for the data: single (e.g. 14) or range (e.g. 01-20)")
	country := flag.String("c", "", "Country code for crux source (e.g. 'global', 'us', 'gb', ...)")
	parse := flag.Bool("parse", false, "Parse all Parquet files in the current directory and print domain names in response_name column")
	ariaBinary := flag.String("aria-binary", "aria2c", "Path to aria2c binary")
	rpcAddr := flag.String("rpc", "http://127.0.0.1:6800/jsonrpc", "aria2c RPC server address")
	rpcToken := flag.String("token", "", "aria2c RPC token")
	downloadDir := flag.String("dir", ".", "Download directory")
	showUrls := flag.Bool("l", false, "List all generated download URLs instead of downloading (with all other provided flags)")

	flag.Parse()

	if *parse {
		parseParquetFiles(*country)
		return
	}
	if *source == "" {
		printHelp()
	}

	validSources := map[string]bool{
		"alexa":    true,
		"umbrella": true,
		"tranco":   true,
		"radar":    true,
		"majestic": true,
		"crux":     true,
	}
	if !validSources[*source] {
		fmt.Printf("Invalid source: %s\n", *source)
		printHelp()
	}
	if *source == "crux" && *country == "" {
		fmt.Println("Country code (-c) is required for source=crux")
		printHelp()
	}

	years := parseYearRange(*year)
	months := parseMonthRange(*month)
	days := parseDayRange(*day)
	allUrls := []string{}

	if *source == "crux" {
		for _, y := range years {
			for _, m := range months {
				baseListURL := "https://openintel.nl/download/forward-dns/basis%3Dtoplist/source%3Dcrux"
				dirRoot := fmt.Sprintf("%s/country-code%%3D%s/year%%3D%s/month%%3D%s/", baseListURL, *country, y, m)
				parquetPattern := `https:\/\/object\.openintel\.nl\/openintel-public\/fdns\/basis=toplist\/source=crux\/country-code=` +
					regexp.QuoteMeta(*country) +
					`\/year=` + regexp.QuoteMeta(y) +
					`\/month=` + regexp.QuoteMeta(m) +
					`\/day=[0-9]{2}\/part-[^"]+\.parquet`
				for _, d := range days {
					dayDir := dirRoot + "day%3D" + d + "/"
					content, err := fetchURL(dayDir)
					if err != nil {
						continue
					}
					urls := extractURLs(content, parquetPattern)
					allUrls = append(allUrls, urls...)
				}
			}
		}
		log.Printf("Found %d URLs to download for crux/country-code=%s", len(allUrls), *country)
	} else {
		baseURL := "https://openintel.nl"
		for _, y := range years {
			for _, m := range months {
				monthRelPath := fmt.Sprintf("/download/forward-dns/basis=toplist/source=%s/year=%s/month=%s", *source, y, m)
				allMonthContent, err := fetchURL(baseURL + monthRelPath)
				if err != nil {
					fmt.Println("Error fetching month URLs:", err)
					continue
				}
				for _, d := range days {
					dayPattern := fmt.Sprintf(`/download/forward-dns/basis=toplist/source=%s/year=%s/month=%s/day=%s`, *source, y, m, d)
					allDayRelativeURLs := extractURLs(allMonthContent, dayPattern)
					for _, dayURL := range allDayRelativeURLs {
						parquetContent, err := fetchURL(baseURL + dayURL)
						if err != nil {
							fmt.Println("Error fetching parquet URLs:", err)
							continue
						}
						parquetPattern := `https:\/\/o.*parquet`
						parquetURLs := extractURLs(parquetContent, parquetPattern)
						allUrls = append(allUrls, parquetURLs...)
					}
				}
			}
		}
		log.Printf("Found %d URLs to download", len(allUrls))
	}

	if *showUrls {
		for _, url := range allUrls {
			fmt.Println(url)
		}
		fmt.Printf("\nTotal %d URLs generated\n", len(allUrls))
		return
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		shutdownServer()
		os.Exit(0)
	}()
	defer shutdownServer()

	ok := checkServer(*rpcAddr, *rpcToken)
	if !ok {
		addr, proc, err := startAria2Server(*ariaBinary, *rpcToken, *downloadDir, *rpcAddr)
		if err != nil {
			log.Fatalf("Unable to start aria2c: %v", err)
		}
		ariaProcess = proc
		*rpcAddr = addr
		log.Printf("Started aria2c (PID %d), waiting for RPC...", ariaProcess.Process.Pid)
		waitForServer(*rpcAddr, *rpcToken)
	}
	log.Printf("Connected to aria2c RPC server at %s", *rpcAddr)

	queue := make(chan string, len(allUrls))
	wg := &sync.WaitGroup{}
	for w := 0; w < maxConcurrentDownloads; w++ {
		wg.Add(1)
		go worker(*rpcAddr, *rpcToken, queue, wg, w)
	}
	for _, url := range allUrls {
		queue <- url
	}
	close(queue)
	wg.Wait()

	log.Printf("All downloads finished. Shutting down aria2c RPC server...")
	sendShutdown(*rpcAddr, *rpcToken)
	if ariaProcess != nil {
		ariaProcess.Wait()
	}
}
