package main

import (
	"bufio"
	"container/list"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strings"
	"time"

	"../redis"
)

type (
	Client struct {
		connection     *rpc.Client
		transactionLog *list.List
	}
)

func NewClient(dsn string, timeout time.Duration) (*Client, error) {
	connection, err := net.DialTimeout("tcp", dsn, timeout)
	if err != nil {
		return nil, err
	}
	return &Client{connection: rpc.NewClient(connection), transactionLog: list.New()}, nil
}

func (c *Client) Get(key string) *redis.CacheItem {
	var item *redis.CacheItem
	if err := c.connection.Call("Redis.Get", key, &item); err != nil {
		fmt.Println(err)
	}
	return item
}

func (c *Client) Set(item *redis.CacheItem) {
	if c.hasOpenTransaction() {
		var currentItem *redis.CacheItem
		if err := c.connection.Call("Redis.Get", item.Key, &currentItem); err == nil {
			c.transactionLog.Back().Value.(*list.List).PushBack(redis.LogItem{Command: "SET", CacheItem: *currentItem})
		} else {
			c.transactionLog.Back().Value.(*list.List).PushBack(redis.LogItem{Command: "UNSET", CacheItem: redis.CacheItem{Key: item.Key}})
		}
	}
	var added bool
	if err := c.connection.Call("Redis.Set", item, &added); err != nil {
		fmt.Println(err)
	}
}

func (c *Client) Unset(key string) {
	if c.hasOpenTransaction() {
		var currentItem *redis.CacheItem
		if err := c.connection.Call("Redis.Get", key, &currentItem); err == nil {
			c.transactionLog.Back().Value.(*list.List).PushBack(redis.LogItem{Command: "SET", CacheItem: *currentItem})
		}
	}
	var deleted bool
	if err := c.connection.Call("Redis.Unset", key, &deleted); err != nil {
		fmt.Println(err)
	}
}

func (c *Client) GetCount(value string) int {
	var count int
	if err := c.connection.Call("Redis.GetCount", value, &count); err != nil {
		fmt.Println(err)
	}
	return count
}

func (c *Client) Begin() {
	c.transactionLog.PushBack(list.New())
}

func (c *Client) Rollback() {
	if !c.hasOpenTransaction() {
		fmt.Println("NO TRANSACTION")
		return
	}
	lastLog := c.transactionLog.Back()
	c.transactionLog.Remove(lastLog)
	log := lastLog.Value.(*list.List)
	sliceToSend := make([]redis.LogItem, log.Len(), log.Len())
	i := 0
	for e := log.Back(); e != nil; e = e.Prev() {
		sliceToSend[i] = e.Value.(redis.LogItem)
		i++
	}
	var done bool
	if err := c.connection.Call("Redis.ExecuteLog", sliceToSend, &done); err != nil {
		fmt.Println(err)
	}
}

func (c *Client) Commit() {
	if c.hasOpenTransaction() {
		lastLog := c.transactionLog.Back()
		c.transactionLog.Remove(lastLog)
		if c.hasOpenTransaction() {
			c.transactionLog.Back().Value.(*list.List).PushBackList(lastLog.Value.(*list.List))
		}
	}
}

func (c *Client) hasOpenTransaction() bool {
	return c.transactionLog.Len() != 0
}

func (c *Client) RollbackLeftOutTransactions() {
	if !c.hasOpenTransaction() {
		return
	}
	for l := c.transactionLog.Back(); l.Prev() != nil; l = l.Prev() {
		l.Prev().Value.(*list.List).PushBackList(l.Value.(*list.List))
	}
	log := c.transactionLog.Front().Value.(*list.List)
	sliceToSend := make([]redis.LogItem, log.Len(), log.Len())
	i := 0
	for e := log.Back(); e != nil; e = e.Prev() {
		sliceToSend[i] = e.Value.(redis.LogItem)
	}
	var done bool
	if err := c.connection.Call("Redis.ExecuteLog", sliceToSend, &done); err != nil {
		fmt.Println(err)
	}
}

func main() {
	c, err := NewClient("localhost:4242", time.Millisecond*500)
	if err != nil {
		fmt.Println("Could not connect to server: ", err)
		return
	}
	defer c.connection.Close()
	var args []string
	reader := bufio.NewReader(os.Stdin)
	for {
		command, _ := reader.ReadString('\n')
		args = strings.Split(strings.Trim(command, "\r\n"+string(0)), " ")
		numOfArgs := len(args)
		if numOfArgs < 1 || numOfArgs > 3 {
			fmt.Println("invalid command")
			continue
		}
		switch strings.ToLower(args[0]) {
		case "set":
			if numOfArgs != 3 {
				fmt.Println("SET needs three arguments")
			} else {
				c.Set(&redis.CacheItem{Key: args[1], Value: args[2]})
			}

		case "get":
			if numOfArgs != 2 {
				fmt.Println("GET needs two arguments")
			} else if returned := c.Get(args[1]); returned != nil {
				fmt.Println(returned.Value)
			}

		case "unset":
			if numOfArgs != 2 {
				fmt.Println("GET needs two arguments")
			} else {
				c.Unset(args[1])
			}

		case "numequalto":
			if numOfArgs != 2 {
				fmt.Println("NUMEQUALTO needs two arguments")
			} else {
				fmt.Println(c.GetCount(args[1]))
			}

		case "begin":
			if numOfArgs != 1 {
				fmt.Println("BEGIN needs one argument only")
			} else {
				c.Begin()
			}

		case "commit":
			if numOfArgs != 1 {
				fmt.Println("COMMIT needs one argument only")
			} else {
				c.Commit()
			}

		case "rollback":
			if numOfArgs != 1 {
				fmt.Println("ROLLBACK needs one argument only")
			} else {
				c.Rollback()
			}

		case "end":
			c.RollbackLeftOutTransactions()
			return

		default:
			fmt.Println("invalid command")
		}
	}
}
