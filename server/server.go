package main

import (
	"log"
	"net"
	"net/rpc"

	"../redis"
)

func main() {
	rpc.Register(redis.NewRedis())

	l, e := net.Listen("tcp", ":4242")
	if e != nil {
		log.Fatal("listen error:", e)
	}

	rpc.Accept(l)
}
