# Raft Key-Value Store in Golang

![Go](https://img.shields.io/badge/Go-1.20+-blue)
![Distributed](https://img.shields.io/badge/Distributed-System-orange)
![Consensus](https://img.shields.io/badge/Raft-Consensus-purple)
![Persistence](https://img.shields.io/badge/Persistence-Supported-green)

A distributed, fault-tolerant **key-value store** implemented in **Golang**, built on the Raft consensus algorithm. This system ensures consistency across nodes, handles leader elections, and persists logs to disk.

---

## ⚙️ Features

- ⛵ **Raft Consensus**: Leader election and log replication
- 🧠 **Strong Consistency**: Ensures a single source of truth across nodes
- 💾 **Persistence**: State survives node crashes via disk storage
- 🔁 **Cluster Communication**: Nodes communicate over HTTP/RPC
- 🧪 **Fault Tolerance**: Survives leader failure, recovers automatically

---

## 🧰 Tech Stack

- **Golang 1.20+**
- **Custom Raft implementation** (no external Raft libraries)
- **Persistent log files** for durability
- **gRPC / HTTP** (for inter-node communication, if used)
- Optional: JSON API or CLI interface

---

## 📂 Project Structure

