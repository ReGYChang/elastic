package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	es "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/rs/zerolog/log"
)

var (
	// Used to create a singleton object of Elasticsearch client.
	// Initialized and exposed through GetClient().
	client *es.Client

	// Used to execute client creation procedure only once.
	once sync.Once

	// Config for Elasticsearch client
	addresses []string
	username  string
	password  string
)

func GetClient() (*es.Client, error) {
	var err error

	once.Do(func() {
		cfg := es.Config{
			Addresses: addresses,
			Username:  username,
			Password:  password,
		}
		client, err = es.NewClient(cfg)
		if err != nil {
			log.Fatal().Err(err).Msg("Error creating the elasticsearch client")
			return
		}
	})

	return client, err
}

func IndexRequest(client *es.Client, req *esapi.IndexRequest) (interface{}, error) {
	var resBody map[string]interface{}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		err := json.NewDecoder(res.Body).Decode(&resBody)
		if err != nil {
			log.Error().Msgf("Error parsing the response body: %s", err)
		}
		return nil, errors.New("error indexing the documents")
	}

	// Deserialize the response into a map.
	err = json.NewDecoder(res.Body).Decode(&resBody)
	if err != nil {
		log.Error().Msgf("Error parsing the response body: %s", err)
		return nil, err
	} else {
		// Print the response status and indexed document version.
		log.Info().Msgf("status=%s; result=%s; version=%d", res.Status(), resBody["result"], int(resBody["_version"].(float64)))
	}

	return resBody["_id"].(string), nil
}

func SearchRequest(client *es.Client, req *esapi.SearchRequest) ([]*SearchHit, error) {
	res, err := req.Do(context.Background(), client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	searchResult := &SearchResult{}
	if err = json.NewDecoder(res.Body).Decode(searchResult); err != nil {
		searchResult.Header = res.Header
		return nil, err
	}

	// error handle
	if res.IsError() {
		return nil, &Error{Status: res.StatusCode, Details: searchResult.Error}
	} else if searchResult.TotalHits() == 0 {
		return nil, errors.New("elastic: found no documents")
	}

	return searchResult.Hits.Hits, nil
}

func UpdateRequest(client *es.Client, req *esapi.UpdateRequest) error {
	res, err := req.Do(context.Background(), client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	updateResponse := &UpdateResponse{}
	if err = json.NewDecoder(res.Body).Decode(updateResponse); err != nil {
		return err
	}

	// error handle
	if res.IsError() {
		return errors.New(updateResponse.Result)
	}

	return nil
}

func DeleteRequest(client *es.Client, req *esapi.DeleteRequest) error {
	res, err := req.Do(context.Background(), client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		err := json.NewDecoder(res.Body).Decode(&e)
		if err != nil {
			log.Error().Msgf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and indexed document version.
			log.Error().Msgf("status=%s; result=%s; version=%d", res.Status(), e["result"], int(e["_version"].(float64)))
		}
		return errors.New("error deleting the documents")
	}

	return nil
}
