Dynamo-style Distributed Key-Value Storage System

This project is about implementing a simplified version of Dynamo.  There are three main tasks that were implemented:
- ID space Partitioning
- Replication
- Failure handling

The main goal is to provide both availability and linearizability at the same time. The implementation always performs read and write operations successfully even under failures. At the same time, a read operation should always return the most recent value. A key-value pair is replicated over three consecutive partitions, starting from the partition that the key belongs to. All replicas store the same value for each key, i.e., “per-key” consistency is accomplished. When a coordinator for a request fails and it does not respond to the request, its successor is contacted next for the request and the key-value is retrieved.
