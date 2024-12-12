package main

import (
	"context"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func insert() {
	// Configure etcd client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"}, // Update this if your etcd endpoint differs
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	log.Println("Connected to etcd")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	start := time.Now()

	const batchSize = 128 // Adjust batch size to the etcd limit
	batch := make([]clientv3.Op, 0, batchSize)

	for i := 1; i <= 10000000; i++ {
		key := fmt.Sprintf("user:%d", i)
		value := fmt.Sprintf("%d", i)

		batch = append(batch, clientv3.OpPut(key, value))

		// Execute batch when it reaches the batch size
		if len(batch) == batchSize {
			_, err := cli.Txn(ctx).Then(batch...).Commit()
			if err != nil {
				log.Printf("Failed to insert batch ending at key user:%d: %v", i, err)
			}

			batch = batch[:0] // Clear the batch
		}

		// Log progress every 1 million keys
		if i%1000000 == 0 {
			log.Printf("Inserted %d keys", i)
		}
	}

	// Insert remaining keys in the last batch
	if len(batch) > 0 {
		_, err := cli.Txn(ctx).Then(batch...).Commit()
		if err != nil {
			log.Printf("Failed to insert final batch: %v", err)
		}
	}

	elapsed := time.Since(start)
	log.Printf("Completed insertion of 10 million keys in %s", elapsed)
}
