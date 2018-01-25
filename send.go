package clickhouse_compress

var httpClient = &http.Client{
	Transport: &http.Transport{MaxIdleConnsPerHost: 1},
	Timeout:   time.Minute,
}

// ToClickHouse sends data to clickhouse and returns errors if any
func Send(host string, table string, body []byte) error {
	body = Compress(body)
	start := time.Now()

	queryPrefix := url.PathEscape(fmt.Sprintf("INSERT INTO %s VALUES", table))
	resp, err := httpClient.Post(fmt.Sprintf("http://%s/?decompress=1&http_native_compression_disable_checksumming_on_decompress=1&query=%s", host, queryPrefix), "application/x-www-form-urlencoded", bytes.NewReader(body))
	if err != nil {
		log.Printf("Could not post to table %s to clickhouse: %s", table, err.Error())
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyText, _ := ioutil.ReadAll(resp.Body)
		log.Printf("Could not post to table %s to clickhouse: %s", table, bodyText)
		// log.Fatalf("Failed query: %s", &b)
		return fmt.Errorf("ClickHouse server returned HTTP code %d", resp.StatusCode)
	}

	io.Copy(ioutil.Discard, resp.Body) // keepalive
	return nil
}
