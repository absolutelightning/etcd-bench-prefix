package main

import (
	"context"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	// Configure etcd client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"}, // Update this if your etcd endpoint differs
		DialTimeout: 5 * time.Minute,
	})
	if err != nil {
		log.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	log.Println("Connected to etcd")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	prefixes := []string{"user:", "user:1", "user:10", "user:100", "user:1000", "user:10000", "user:100000", "user:1000000", "user:10000000"}

	for _, prefix := range prefixes {
		start := time.Now()
		resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
		if err != nil {
			log.Printf("Failed to get keys with prefix %q: %v", prefix, err)
			continue
		}
		for _, kv := range resp.Kvs {
			fmt.Println(string(kv.Key))
		}
		fmt.Println(prefix, "->", time.Since(start))
	}

	log.Println("Completed querying prefixes")

}
