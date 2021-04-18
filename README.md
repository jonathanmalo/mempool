# mempool
Simple go utility that stores 'newPendingTransactions' via IPC in elasticsearch while deleting transactions found in 'newHeads' from elasticsearch. Makes the 'data' transaction field searchable in elasticsearch and removes fields related to blocks. Credit to https://github.com/taarushv/helios for most of the code.
## Requirements
Tested with 16gb RAM, full geth node v1.10.3-unstable, elasticsearch v7.12
Go dependencies: 
github.com/ethereum/go-ethereum/common
github.com/ethereum/go-ethereum/core/types
github.com/ethereum/go-ethereum/ethclient
github.com/ethereum/go-ethereum/rpc
github.com/elastic/go-elasticsearch/v7
github.com/elastic/go-elasticsearch/v7/esapi
