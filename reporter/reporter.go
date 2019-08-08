package reporter

import (
	"encoding/json"
	"fmt"
)

var report = map[string]interface{}{}

// Commit values to the report
// To print values to stdout use Write function
func Commit(key string, value interface{}) {
	report[key] = value
}

// Get value that was previously committed
func Get(key string) (interface{}, bool) {
	val, ok := report[key]
	return val, ok
}

// Increment the value under the specified key
// Works for int values only
// Returns the new value of the counter.
func Increment(key string) int {
	if _, exists := report[key]; !exists {
		report[key] = 0
	}
	report[key] = report[key].(int) + 1
	return report[key].(int)
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
	report = map[string]interface{}{}
}
