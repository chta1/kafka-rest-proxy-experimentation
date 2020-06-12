package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

const data = `
{
  "value_schema_id": 62, 
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

	avroContentType := "application/vnd.kafka.avro.v2+json"
	localHost := "http://localhost:8082"
	localRegistryHost := "http://localhost:8081"

	registerSchema(localRegistryHost, "loan.transaction.applied.v1", schema)
	getSchemaSubject(localRegistryHost, "loan.transaction.applied.v1-value")
	postToTopic(localHost, "transactions", avroContentType, data)

	// update rest proxy host and registry from values here
    // https://talamobile.atlassian.net/wiki/spaces/DEV/pages/1027866786/Atlas+MSK+-+Kafka+Infrastructure+details
	devHost := "rest-proxy-host:8082"
	devRegistryHost := "schema-registry:8081"

	listTopics(devHost)
	getTopic(devHost, "dev-india-etl-comms-message-statuses")
	listSchemaSubjects(devRegistryHost)
	getSchemaSubject(devRegistryHost, "data_warehouse-value")
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

func registerSchema(host, subjectWithoutValue, rSchema string) {
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
	log.Printf("*** POST new schema to host=[%s], subjectValue=[%s]. response=[%s]\n\n", host, subjectWithoutValue, schemaRespBody)
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


