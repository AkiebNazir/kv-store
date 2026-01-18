package main

import (
	"fmt"
	"net/http"
	"time"
)

// CONFIGURATION
const (
	TotalKeys   = 2000
	Concurrency = 10
	TargetURL   = "http://localhost:8081/get?key="
)

func main() {
	fmt.Printf(" Starting population of %d keys to %s...\n", TotalKeys, TargetURL)
	start := time.Now()
	for i := 1; i <= TotalKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		resp, err := http.Get(TargetURL + key)
		if err != nil {
			fmt.Printf("Error key-%d: %v\n", i, err)
			return
		} else {
			defer resp.Body.Close()
			fmt.Printf("Pass: %s", resp.Status)

		}
		// val := fmt.Sprintf("value-payload-%d", i)
		// body := []byte(fmt.Sprintf(`{"key":"%s", "value":"%s"}`, key, val))

		// resp, err := http.Post(TargetURL, "application/json", bytes.NewBuffer(body))
		// if err != nil {
		// 	fmt.Printf("Error key-%d: %v\n", i, err)
		// 	return
		// }
		// defer resp.Body.Close()

		// if resp.StatusCode != 200 {
		// 	fmt.Printf("Failed key-%d: Status %d\n", i, resp.StatusCode)
		// } else if i%100 == 0 {
		// 	fmt.Printf(".") // Progress dot
		// }
	}
	fmt.Printf("\nDone! Inserted %d keys in %s\n", TotalKeys, time.Since(start))
}
