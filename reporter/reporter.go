package reporter

import (
	"encoding/json"
	"fmt"
)

var report = map[string]int{}

// Commit values to the report
// To print values to stdout use Write function
func Commit(key string, value int) {
	report[key] = value
}

// Get value that was previously committed
func Get(key string) (int, bool) {
	val, ok := report[key]
	return val, ok
}

// Increment the value under the specified key
// Returns the new value of the counter.
func Increment(key string) int {
	report[key]++
	return report[key]
}

// Write function prints report to stdout and clear all values
func Write() {
	if jsonString, err := json.Marshal(report); err == nil {
		fmt.Println(string(jsonString))
	} else {
		panic(err)
	}
}

// Reset sets all the counter values to 0
func Reset() {
	report = map[string]int{}
}
