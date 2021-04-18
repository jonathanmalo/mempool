package services

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"log"
	"math/big"
	"strings"
	"time"
)

func getTxSenderAddress(tx *types.Transaction, client *ethclient.Client) string {
	chainID, err := client.NetworkID(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	msg, _ := tx.AsMessage(types.NewEIP155Signer(chainID))
	return msg.From().Hex()
}

func formatEthWeiToEther(etherAmount *big.Int) float64 {
	var base, exponent = big.NewInt(10), big.NewInt(18)
	denominator := base.Exp(base, exponent, nil)
	// Convert to float for precision
	tokensSentFloat := new(big.Float).SetInt(etherAmount)
	denominatorFloat := new(big.Float).SetInt(denominator)
	// Divide and return the final result
	final, _ := new(big.Float).Quo(tokensSentFloat, denominatorFloat).Float64()
	return final
}

func isTxMined(txHash string, client *ethclient.Client) bool {
	finalTxHash := common.HexToHash(txHash)
	_, isPending, err := client.TransactionByHash(context.Background(), finalTxHash)
	if err != nil {
		log.Fatal(err)
	}
	return !isPending
}

func hasTxFailed(txHash string, client *ethclient.Client) bool {
	if isTxMined(txHash, client) {
		receipt, err := client.TransactionReceipt(context.Background(), common.HexToHash(txHash))
		if err != nil {
			log.Fatal(err)
		}
		if receipt.Status == 1 {
			return false
		} else {
			return true
		}
	} else {
		return false
	}
}

func pipeTransaction(tx *types.Transaction, client *ethclient.Client) {
	if len(tx.Data()) < 4 || tx.To() == nil {
		return
	}
	// Connect to our es client
	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		log.Fatalf("Error connecting to es client: %s", err)
	}
	// Start building a document
	body := struct {
		// Time the tx was discovered and other details (for data analysis + Kibana)
		TimeSeen int64  `json:"timeFirstDiscovered"`
		Hash     string `json:"txHash"`
		// Classifcation and other info
		From     string  `json:"from"`
		To       string  `json:"to"`
		Value    float64 `json:"txValue"`
		Data     string  `json:"data"`
		Nonce    uint64  `json:"nonce"`
		GasPrice float64 `json:"gasPrice"`
		Gas      float64 `json:"gas"`
		// Custom tags that are updated after a block including the tx is mined
		Mined         bool  `json:"txMined"`
		BlockIncluded int64 `json:"blockIncluded"`
		Failed        bool  `json:"txFailed"`
	}{}
	body.TimeSeen = time.Now().Unix()
	body.Hash = tx.Hash().Hex()
	body.From = getTxSenderAddress(tx, client)
	body.To = tx.To().Hex()
	body.Value = formatEthWeiToEther(tx.Value())
	body.Data = hex.EncodeToString(tx.Data())
	body.Nonce = tx.Nonce()
	formattedGas := new(big.Int).SetUint64(tx.Gas())
	body.Gas = formatEthWeiToEther(formattedGas)
	body.GasPrice = formatEthWeiToEther(tx.GasPrice())
	body.Mined = isTxMined(tx.Hash().Hex(), client)
	body.Failed = hasTxFailed(tx.Hash().Hex(), client)
	jsonBytes, _ := json.Marshal(body)
	// Set up the request object.
	req := esapi.IndexRequest{
		Index:   "transactions",
		Body:    bytes.NewReader(jsonBytes),
		Refresh: "true",
	}
	// Perform the request with the client.
	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("[%s] Error indexing document", res.Status())
	} else {
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and indexed document version.
			log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
		}
	}
}

func getTxIDByHash(txHash string) string {
	var (
		r map[string]interface{}
	)
	es, _ := elasticsearch.NewDefaultClient()
	// 3. Search for the indexed documents
	//
	// Build the request body.
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"txHash": txHash,
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}

	// Perform the search request.
	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("transactions"),
		es.Search.WithBody(&buf),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
	)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			log.Fatalf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
	}

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}

	// Print the ID and document source for each hit.
	for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
		return fmt.Sprintf("%v", hit.(map[string]interface{})["_id"])
	}
	return ""
}

func DeleteTx(txHash string, client *ethclient.Client) {
	txID := getTxIDByHash(txHash)
	if txID != "" {
		es, err := elasticsearch.NewDefaultClient()
		if err != nil {
			log.Fatalf("Error connecting to es client: %s", err)
		}
		res, err := es.Delete("transactions", txID)
		if err != nil {
			fmt.Println("Error getting the response:", err)
		}
		defer res.Body.Close()
	}
}

func pipeBlock(block *types.Block, client *ethclient.Client) {
	// TODO: delete mined txs from elasticsearch
	fmt.Println("MINED: Block #", block.Number())
	for _, tx := range block.Transactions() {
		fmt.Println(tx.Hash().Hex())
		DeleteTx(tx.Hash().Hex(), client)
	}
}

// This removes all the data in a given elastic instance
// Equivalent of `rm -rf *`, call this method with caution
func FlushIndexData(indexName string) {
	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		log.Fatalf("Error connecting to es client: %s", err)
	}
	res, err := es.DeleteByQuery(
		[]string{indexName},
		strings.NewReader(`{
		  "query": {
			"match_all": {}
		  }
		}`),
	)
	fmt.Println(res, err)
}
