package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

const data = `
{
  "value_schema_id": %d, 
  "records": [
    {
      "value": {
        "id": "my-id-1", 
        "amount": 15
      }
    }
  ]
}
`

const schema = `{"schema": "{\"namespace\": \"com.tala.loans.model.kafka\",\"type\": \"record\",\"name\": \"Payment\",\"fields\": [{\"name\": \"id\", \"type\": \"string\"},{\"name\": \"amount\", \"type\": \"double\"}]}"}`

type Payment struct {
	ID     string  `avro:"id"`
	Amount float64 `avro:"amount"`
}

func main() {

	//jsonConent := "application/vnd.kafka.v2+json"
	//contentType := "application/vnd.kafka.json.v2+json"
	avroContentType := "application/vnd.kafka.avro.v2+json"
	localHost := "http://localhost:8082"
	postTopic := "transactions1"
	consumerGroup := "my-test-group"
	consumer := "my-consumer5"
	localRegistryHost := "http://localhost:8081"

	id := registerSchema(localRegistryHost, postTopic, schema)
	//id := 62
	//getSchemaSubject(localRegistryHost, postTopic + "-value")

	createConsumer(localHost, avroContentType, consumerGroup, consumer)
	subscribeToTopic(localHost, postTopic, avroContentType, consumerGroup, consumer)

	postToTopic(localHost, postTopic, avroContentType, fmt.Sprintf(data, id))

	getMessage(localHost, consumerGroup, consumer, avroContentType, 5000)
	time.Sleep(3 * time.Second)
	// Don't unsubscribed || destroy if we only want the messages what have not consumed
	//unSubscribedToTopic(localHost, consumerGroup, consumer)
	//destroyConsumer(localHost, consumerGroup, consumer)

	//
	//devHost := "http://cp-kafka-rest-proxy.dev.india.atlas-antelope.com:8082"
	//devRegistryHost := "http://schema-registry.dev.india.atlas-antelope.com:8081"
	//
	//listTopics(devHost)
	//getTopic(devHost, "dev-india-etl-comms-message-statuses")
	//listSchemaSubjects(devRegistryHost)
	//getSchemaSubject(devRegistryHost, "data_warehouse-value")

}

func createConsumer(host, contentType, group, consumer string) {

	content := fmt.Sprintf(
		`{
  			"name": "%s",
  			"format": "avro",
  			"auto.offset.reset": "latest",
  			"auto.commit.enable": "true"
    	}`, consumer)

	resp, err := http.Post(
		fmt.Sprintf("%s/consumers/%s", host, group),
		contentType,
		bytes.NewBufferString(content),
	)

	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("*** Create new consumer on host=[%s], group=[%s], consumerName=[%s]. response=[%s]\n\n", host, group, consumer, body)
}

// Destroy the consumer instance
func destroyConsumer(host, group, consumer string) {
	del := fmt.Sprintf("%s/consumers/%s/instances/%s", host, group, consumer)

	// Create client
	client := &http.Client{}

	// Create request
	req, err := http.NewRequest("DELETE", del, nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	header := map[string][]string{
			"Accept": {"application/vnd.kafka.v2+json"},
	}

	req.Header = header
	// Fetch Request
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Printf("*** DESTROY CONSUMER  url=[%s]. response=[%s]\n\n", del, body)
}

func subscribeToTopic(host, topic, contentType, group, consumer string) {
	content := fmt.Sprintf(`{
  		"topics": [
    		"%s"
	    ]
    }`, topic)

	resp, err := http.Post(
		fmt.Sprintf("%s/consumers/%s/instances/%s/subscription", host, group, consumer),
		contentType,
		bytes.NewBufferString(content),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("*** SUBSCRIBE to host=[%s], group=[%s], consumerName=[%s]. response=[%s]\n\n", host, group, consumer, body)
}

func unSubscribedToTopic(host, group, consumer string) {
	del := fmt.Sprintf("%s/consumers/%s/instances/%s/subscription", host, group, consumer)

	// Create client
	client := &http.Client{}

	// Create request
	req, err := http.NewRequest("DELETE", del, nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Fetch Request
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Printf("*** UNSUBSCRIBE to host=[%s], group=[%s], consumerName=[%s]. response=[%s]\n\n", host, group, consumer, body)
}


func getMessage(host, group, consumer, contentType string, timeout int) {

	get := fmt.Sprintf("%s/consumers/%s/instances/%s/records?timeout=%d", host, group, consumer, timeout)

	// Create client
	client := &http.Client{}

	// Create request
	req, err := http.NewRequest("GET", get, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	header := map[string][]string{
		"Accept": {fmt.Sprintf("%s", contentType)},
	}

	req.Header = header
	// Fetch Request
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Printf("*** GET MESSAGE from host=[%s], group=[%s], consumer=[%s], timeout=[%d]. response=[%s]\n\n", host, group, consumer, timeout, body)
}

func listTopics(host string) {
	listTopicResp, err := http.Get(fmt.Sprintf("%s/topics", host))
	if err != nil {
		log.Fatal(err)
	}
	defer listTopicResp.Body.Close()
	listTopicRespBody, err := ioutil.ReadAll(listTopicResp.Body)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("*** List topic from host=[%s]. response=[%s]\n\n", host, listTopicRespBody)
}

func getTopic(host, topic string) {
	topicResp, err := http.Get(fmt.Sprintf("%s/topics/dev-india-etl-comms-message-statuses", host))
	if err != nil {
		log.Fatal(err)
	}
	defer topicResp.Body.Close()
	topicRespBody, err := ioutil.ReadAll(topicResp.Body)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("*** GET topic from host=[%s], topic=[%s]. response=[%s]\n\n", host, topicRespBody)
}

func postToTopic(host, topic, contentType, content string) {
	resp, err := http.Post(
		fmt.Sprintf("%s/topics/%s", host, topic),
		contentType,
		bytes.NewBufferString(content),
	)

	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("*** Post new record to host=[%s], topic=[%s]. response=[%s]\n\n", host, topic, body)
}

func registerSchema(host, subjectWithoutValue, rSchema string) int {
	// register a schema using rest api
	// default is usually have -value appended
	schemaResp, err := http.Post(
		fmt.Sprintf("%s/subjects/%s-value/versions", host, subjectWithoutValue),
		"application/vnd.schemaregistry.v1+json",
		bytes.NewBufferString(rSchema),
	)

	if err != nil {
		log.Fatal(err)
	}
	defer schemaResp.Body.Close()

	schemaRespBody, err := ioutil.ReadAll(schemaResp.Body)
	if err != nil {
		log.Fatal(err)
	}
	idRegex := regexp.MustCompile("\\d+")
	idStr := idRegex.FindString(string(schemaRespBody))
	log.Printf("*** POST new schema to host=[%s], subjectValue=[%s]. response=[%s]\n\n", host, subjectWithoutValue, schemaRespBody)
	id, _ :=  strconv.Atoi(idStr)
	return id
}

func getSchemaSubject(host, subject string) {
	schemaIDResp, err := http.Get(fmt.Sprintf("%s/subjects/%s/versions/latest", host, subject))
	if err != nil {
		log.Fatal(err)
	}
	defer schemaIDResp.Body.Close()
	schemaIDRespBody, err := ioutil.ReadAll(schemaIDResp.Body)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("*** GET schema for host=[%s] subject=[%s]. response=[%s]\n\n", host, subject, schemaIDRespBody)
}

func listSchemaSubjects(schemaHost string) {
	schemaSubjectListResp, err := http.Get(fmt.Sprintf("%s/subjects", schemaHost))
	if err != nil {
		log.Fatal(err)
	}
	defer schemaSubjectListResp.Body.Close()
	schemaSubjectListRespBody, err := ioutil.ReadAll(schemaSubjectListResp.Body)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("*** List schema subject from host=[%s]. response=[%s]\n\n", schemaHost, schemaSubjectListRespBody)
}

