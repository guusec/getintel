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
█─▄▄▄▄█▄─▄▄─█─▄─▄─█▄─▄█─▄─▄─█▄─▀█▄─▄█▄─▄▄─█▄─▄███
█─██▄─██─▄█▀███─████─████─████─█▄▀─███─▄█▀██─██▀█
▀▄▄▄▄▄▀▄▄▄▄▄▀▀▄▄▄▀▀▄▄▄▀▀▄▄▄▀▀▄▄▄▀▀▄▄▀▄▄▄▄▄▀▄▄▄▄▄▀
	`)
	fmt.Println("Usage: getintel [options]")
	fmt.Println("Options:")
	fmt.Println("  -src string")
	fmt.Println("        Source for the data (options: alexa, umbrella, tranco, radar, majestic, crux)")
	fmt.Println("  -y string")
	fmt.Println("        Year for the data (default is 2025)")
	fmt.Println("  -m string")
	fmt.Println("        Month for the data (default is 03)")
	fmt.Println("  -d string")
	fmt.Println("        Day for the data (optional; start downloading from this day onward)")
	fmt.Println("  -parse")
	fmt.Println("        Parse all Parquet files in the current directory and print domain names in response_name column")
	fmt.Println("  -aria-binary string")
	fmt.Println("        Path to aria2c binary (for download mode) (default 'aria2c')")
	fmt.Println("  -rpc string")
	fmt.Println("        aria2c RPC server address (default 'http://127.0.0.1:6800/jsonrpc')")
	fmt.Println("  -token string")
	fmt.Println("        aria2c RPC token (default empty)")
	fmt.Println("  -dir string")
	fmt.Println("        Download directory (for aria2c)")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  getintel -src=umbrella -y=2025 -m=03 -d=14")
	fmt.Println("  getintel -src=alexa -y=2023 -m=12")
	fmt.Println("  getintel -parse")
	os.Exit(0)
}

func parseParquetFiles() {
	files, err := filepath.Glob("*.parquet")
	if err != nil {
		log.Fatalf("Error finding Parquet files: %v", err)
	}
	if len(files) == 0 {
		fmt.Println("No Parquet files found in current directory")
		return
	}
	quotedFiles := make([]string, len(files))
	for i, f := range files {
		quotedFiles[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(f, "'", "''"))
	}
	fileList := strings.Join(quotedFiles, ", ")
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	query := fmt.Sprintf(`
		SELECT response_name
		FROM read_parquet([%s])
		WHERE response_name IS NOT NULL
	`, fileList)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()
	fmt.Println("Domain names found in Parquet files:")
	var domain string
	count := 0
	for rows.Next() {
		if err := rows.Scan(&domain); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}
		fmt.Println(domain)
		count++
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Error reading rows: %v", err)
	}
	fmt.Printf("\nFound %d domain names\n", count)
}

// pollDownloadCompletion checks a GID until its status is "complete", "error" or "removed"
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
	day := flag.String("d", "", "Day for the data (optional; start downloading from this day onward)")
	parse := flag.Bool("parse", false, "Parse all Parquet files in the current directory and print domain names in response_name column")
	ariaBinary := flag.String("aria-binary", "aria2c", "Path to aria2c binary")
	rpcAddr := flag.String("rpc", "http://127.0.0.1:6800/jsonrpc", "aria2c RPC server address")
	rpcToken := flag.String("token", "", "aria2c RPC token")
	downloadDir := flag.String("dir", ".", "Download directory")

	flag.Parse()

	if *parse {
		parseParquetFiles()
		return
	}
	if *source == "" {
		printHelp()
	}

	// Set up signals and shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		shutdownServer()
		os.Exit(0)
	}()
	defer shutdownServer()

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

	// Aria2c: connect or start
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

	baseURL := "https://openintel.nl"
	yearPattern := fmt.Sprintf(`/download/forward-dns/basis=toplist/source=%s/.*[0-9]`, *source)
	monthPattern := fmt.Sprintf(`/download/forward-dns/basis=toplist/source=%s/year=%s/month=%s`, *source, *year, *month)
	parquetPattern := `https:\/\/o.*parquet`

	allYearContent, err := fetchURL(baseURL + "/download/forward-dns/basis=toplist/source=" + *source + "/")
	if err != nil {
		fmt.Println("Error fetching year URLs:", err)
		return
	}
	allYearRelativeURLs := extractURLs(allYearContent, yearPattern)

	allUrls := []string{}
	for _, yearURL := range allYearRelativeURLs {
		allMonthContent, err := fetchURL(baseURL + yearURL)
		if err != nil {
			fmt.Println("Error fetching month URLs:", err)
			continue
		}
		allMonthRelativeURLs := extractURLs(allMonthContent, monthPattern)
		for _, monthURL := range allMonthRelativeURLs {
			if *day != "" {
				allDayContent, err := fetchURL(baseURL + monthURL)
				if err != nil {
					fmt.Println("Error fetching day URLs:", err)
					continue
				}
				dayPattern := fmt.Sprintf(`/download/forward-dns/basis=toplist/source=%s/year=%s/month=%s/day=([0-9]{2})`, *source, *year, *month)
				allDayRelativeURLs := extractURLs(allDayContent, dayPattern)
				for _, dayURL := range allDayRelativeURLs {
					// Extract NN from /day=NN
					idx := strings.Index(dayURL, "/day=")
					if idx == -1 || len(dayURL) < idx+7 {
						continue
					}
					fileDayStr := dayURL[idx+5 : idx+7]
					fileDay := 0
					userDay := 0
					fmt.Sscanf(fileDayStr, "%02d", &fileDay)
					fmt.Sscanf(*day, "%02d", &userDay)
					if fileDay < userDay {
						continue
					}
					parquetContent, err := fetchURL(baseURL + dayURL)
					if err != nil {
						fmt.Println("Error fetching parquet URLs:", err)
						continue
					}
					parquetURLs := extractURLs(parquetContent, parquetPattern)
					allUrls = append(allUrls, parquetURLs...)
				}
			} else {
				parquetContent, err := fetchURL(baseURL + monthURL)
				if err != nil {
					fmt.Println("Error fetching parquet URLs for month:", err)
					continue
				}
				parquetURLs := extractURLs(parquetContent, parquetPattern)
				allUrls = append(allUrls, parquetURLs...)
			}
		}
	}
	log.Printf("Found %d URLs to download", len(allUrls))

	// Limit concurrency: only 4 workers, each does not add a new download until their previous finished
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
