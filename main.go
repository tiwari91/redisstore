package main

import (
	"bufio"
	"fmt"
	"net"
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
		db.data[key] = "0"
		val = "0"
	}

	current, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ERR value is not an integer")
	}

	current += by
	db.data[key] = strconv.FormatInt(current, 10)
	return current, nil
}

func isValidValue(value string) bool {
	return strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")
}

func handleClient(conn net.Conn, db *KeyValueDB) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		cmd, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading command:", err)
			return
		}
		cmd = strings.TrimSpace(cmd)
		parts := strings.Fields(cmd)
		if len(parts) == 0 {
			continue
		}

		command := strings.ToUpper(parts[0])

		switch command {
		case "SET":
			if len(parts) < 3 {
				conn.Write([]byte("Usage: SET <key> <value>\n"))
				continue
			}
			key := parts[1]
			value := strings.Join(parts[2:], " ")

			if _, err := strconv.Atoi(value); err == nil {
				_, err := db.Incr(key, 0)
				if err != nil {
					conn.Write([]byte(fmt.Sprintf("%s\n", err.Error())))
					continue
				}
				conn.Write([]byte("OK\n"))
				continue
			}

			// If not a number, set the value as usual
			if err := db.Set(key, value); err != nil {
				conn.Write([]byte(fmt.Sprintf("%s\n", err.Error())))
				continue
			}
			conn.Write([]byte("OK\n"))
		case "GET":
			if len(parts) < 2 {
				conn.Write([]byte("Usage: GET <key>\n"))
				continue
			}
			key := parts[1]
			val, ok := db.Get(key)
			if ok {
				conn.Write([]byte(fmt.Sprintf("%q\n", val)))
			} else {
				conn.Write([]byte("(nil)\n"))
			}
		case "DELETE":
			if len(parts) < 2 {
				conn.Write([]byte("Usage: DELETE <key>\n"))
				continue
			}
			key := parts[1]
			if db.Delete(key) {
				conn.Write([]byte("(integer) 1\n"))
			} else {
				conn.Write([]byte("(integer) 0\n"))
			}
		case "INCR":
			if len(parts) < 2 {
				conn.Write([]byte("Usage: INCR <key>\n"))
				continue
			}
			key := parts[1]
			_, err := db.Incr(key, 1)
			if err != nil {
				conn.Write([]byte(fmt.Sprintf("%s\n", err.Error())))
				continue
			}
			conn.Write([]byte("OK\n"))
		case "INCRBY":
			if len(parts) < 3 {
				conn.Write([]byte("Usage: INCRBY <key> <increment>\n"))
				continue
			}
			key := parts[1]
			incrBy, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				conn.Write([]byte("ERR invalid increment\n"))
				continue
			}
			_, err = db.Incr(key, incrBy)
			if err != nil {
				conn.Write([]byte(fmt.Sprintf("%s\n", err.Error())))
				continue
			}
			conn.Write([]byte(fmt.Sprintf("(integer) %d\n", incrBy)))
		case "DISCONNECT":
			return
		default:
			conn.Write([]byte(fmt.Sprintf("Unknown command: %s\n", command)))
		}
	}
}

func main() {
	port := 4544
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	defer listener.Close()

	db := NewKeyValueDB()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go handleClient(conn, db)
	}
}
