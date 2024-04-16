package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

// KeyValueDB represents an in-memory key-value database
type KeyValueDB struct {
	data map[string]string
	mu   sync.RWMutex
}

// NewKeyValueDB creates a new instance of KeyValueDB
func NewKeyValueDB() *KeyValueDB {
	return &KeyValueDB{
		data: make(map[string]string),
	}
}

// Set sets the value of a key in the database
func (db *KeyValueDB) Set(key, value string) error {
	if !isValidValue(value) {
		return fmt.Errorf("ERR syntax error: Value should be enclosed in quotes")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data[key] = value
	return nil
}

// Get retrieves the value of a key from the database
func (db *KeyValueDB) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	val, ok := db.data[key]
	return val, ok
}

// Delete deletes a key from the database
func (db *KeyValueDB) Delete(key string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, ok := db.data[key]
	if ok {
		delete(db.data, key)
		return true
	}
	return false
}

func isValidValue(value string) bool {
	return strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")
}

func main() {
	db := NewKeyValueDB()

	// Start accepting commands from the command line
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		scanned := scanner.Scan()
		if !scanned {
			return
		}

		line := scanner.Text()
		parts := strings.Fields(line)

		if len(parts) == 0 {
			continue
		}

		command := strings.ToUpper(parts[0])

		switch command {
		case "SET":
			if len(parts) < 3 {
				fmt.Println("Usage: SET <key> <value>")
				continue
			}
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			if err := db.Set(key, value); err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("OK")
		case "GET":
			if len(parts) < 2 {
				fmt.Println("Usage: GET <key>")
				continue
			}
			key := parts[1]
			val, ok := db.Get(key)
			if ok {
				fmt.Printf("%q\n", val)
			} else {
				fmt.Println("(nil)")
			}
		case "DELETE":
			if len(parts) < 2 {
				fmt.Println("Usage: DELETE <key>")
				continue
			}
			key := parts[1]
			if db.Delete(key) {
				fmt.Println("(integer) 1")
			} else {
				fmt.Println("(integer) 0")
			}
		default:
			fmt.Println("Unknown command:", command)
		}
	}
}
