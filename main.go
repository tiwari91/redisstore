package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

type KeyValueDB struct {
	data map[string]string
	mu   sync.RWMutex
}

func NewKeyValueDB() *KeyValueDB {
	return &KeyValueDB{
		data: make(map[string]string),
	}
}

func (db *KeyValueDB) Set(key, value string) error {
	if !isValidValue(value) {
		return fmt.Errorf("ERR syntax error: Value should be enclosed in quotes")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data[key] = value
	return nil
}

func (db *KeyValueDB) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	val, ok := db.data[key]
	return val, ok
}

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

func (db *KeyValueDB) Incr(key string, by int64) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	val, ok := db.data[key]
	if !ok {
		// If the key doesn't exist, initialize it with 0
		db.data[key] = "0"
		val = "0"
	}

	// Parse the existing value as an integer
	current, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ERR value is not an integer")
	}

	// Increment the value by the specified amount
	current += by
	db.data[key] = strconv.FormatInt(current, 10)
	return current, nil
}

func isValidValue(value string) bool {
	return strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")
}

func main() {
	db := NewKeyValueDB()

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

			if _, err := strconv.Atoi(value); err == nil {
				_, err := db.Incr(key, 0)
				if err != nil {
					fmt.Println(err)
					continue
				}
				fmt.Println("OK")
				continue
			}

			// If not a number, set the value as usual
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
		case "INCR":
			if len(parts) < 2 {
				fmt.Println("Usage: INCR <key>")
				continue
			}
			key := parts[1]
			_, err := db.Incr(key, 1)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("OK")
		case "INCRBY":
			if len(parts) < 3 {
				fmt.Println("Usage: INCRBY <key> <increment>")
				continue
			}
			key := parts[1]
			incrBy, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				fmt.Println("ERR invalid increment")
				continue
			}
			_, err = db.Incr(key, incrBy)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("(integer) %d\n", incrBy)
		default:
			fmt.Println("Unknown command:", command)
		}
	}
}
