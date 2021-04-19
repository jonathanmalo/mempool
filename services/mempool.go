package services

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"log"
	"os"
)

func StreamNewTxs(rpcClient *rpc.Client) {
	newTxsChannel := make(chan common.Hash)

	rpcClient.EthSubscribe(
		context.Background(), newTxsChannel, "newPendingTransactions", // no additional args
	)
	fmt.Println("Susbscribed to new pending transactions")
	client := ethclient.NewClient(rpcClient)
	// Configure chain ID and signer to ensure you're configured to mainnet
	chainID, _ := client.NetworkID(context.Background())
	signer := types.NewEIP155Signer(chainID)
	for {
		select {
		// Code block is executed when a new tx hash is piped to the channel
		case transactionHash := <-newTxsChannel:
			// Get transaction object from hash by querying the client
			tx, is_pending, _ := client.TransactionByHash(context.Background(), transactionHash)
			// If tx is valid and still unconfirmed
			// TODO: evm execution check
			if is_pending {
				_, _ = signer.Sender(tx)
				pipeTransaction(tx, client)
			}
		}
	}
}

func handleBlock(blockHash common.Hash, client *ethclient.Client) {
	block, err := client.BlockByHash(context.Background(), blockHash)
	if err != nil {
		log.Fatal(err)
	}
	pipeBlock(block)
}

func StreamNewBlocks(rpcClient *rpc.Client) {
	// Go channel to pipe data from client subscriptions
	newBlocksChannel := make(chan *types.Header, 10)

	// Subscribe to receive one time events for new txs
	// i.e Pipe new data to the channel every time a block is mined
	rpcClient.EthSubscribe(
		context.Background(), newBlocksChannel, "newHeads", // no additional args
	)

	fmt.Println("Subscribed to new blocks")
	client := ethclient.NewClient(rpcClient)
	for {
		select {
		// Code block is executed when a new block is piped to the channel
		case lastBlockHeader := <-newBlocksChannel:
			func() {
				go handleBlock(lastBlockHeader.Hash(), client)
			}()
		}
	}
}

func getRpcClient(url string) *rpc.Client {
	client, err := rpc.Dial(url)
	if err != nil {
		log.Fatalf(err.Error())
	}
	return client
}

func StreamMempool() {
	localIpc := os.Getenv("HOME") + "/.ethereum/geth.ipc"
	client := getRpcClient(localIpc)
	fmt.Println("Streaming mempool over IPC socket: ", localIpc)
	go StreamNewTxs(client)
	go StreamNewBlocks(client)
	for {
	}
}
