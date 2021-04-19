package services

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
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
		}
		// else {
		// 	// Print the response status and indexed document version.
		// 	log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
		// }
	}
}

func DeleteTx(txHash string) int {
	es, _ := elasticsearch.NewDefaultClient()
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
	// fmt.Printf("%v", res)
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}

	if r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64) == 0.0 {
		return 0
	}

	for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
		txId := hit.(map[string]interface{})["_id"].(string)
		// fmt.Printf("\nDeleting %s\n", txId)
		res, err := es.Delete("transactions", txId)
		if err != nil {
			fmt.Println("Error getting the response:", err)
		}
		defer res.Body.Close()
	}
	return 1
}

func pipeBlock(block *types.Block) {
	// delete mined txs from elastic search
	// TODO: delete all txs from same account with nonce <= mined tx nonce
	fmt.Println("MINED: Block #", block.Number())
	var count int
	for _, tx := range block.Transactions() {
		// fmt.Printf("\r%s", tx.Hash().Hex())
		count += DeleteTx(tx.Hash().Hex())
	}
	total := len(block.Transactions())
	fmt.Printf("%.2f%% of block transactions were found by this peer (%d of %d)\n", 100.0*float64(count)/float64(total), count, total)
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
