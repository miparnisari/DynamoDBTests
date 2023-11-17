package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

func main() {
	tableName := "testtable"
	_, client, clean := createDynamoDbClientAndTable(tableName)
	defer clean()

	expr, err := expression.NewBuilder().
		WithCondition(expression.AttributeNotExists(expression.Name("PK"))).
		Build()
	if err != nil {
		fmt.Println(err)
		return
	}

	putItemInput1 := &dynamodb.PutItemInput{
		TableName: &tableName,
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{
				Value: "HELLO"},
			"SK": &types.AttributeValueMemberS{
				Value: "V1"}},
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConditionExpression:       expr.Condition(),
	}

	_, err = client.PutItem(context.Background(), putItemInput1)
	if err != nil {
		fmt.Println("Error putting item 1:", err)
		return
	}
	fmt.Println("Item 1 added successfully.")

	putItemInput2 := &dynamodb.PutItemInput{
		TableName: &tableName,
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{
				Value: "HELLO"},
			"SK": &types.AttributeValueMemberS{
				Value: "V2"}},
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConditionExpression:       expr.Condition(),
	}

	resp, err := client.PutItem(context.Background(), putItemInput2)
	if err != nil {
		fmt.Println("Error putting item 2:", err)
		fmt.Println(resp)
		return
	}

	fmt.Println("Item 2 added successfully.")
}

func createDynamoDbClientAndTable(tablename string) (*dockertest.Resource, *dynamodb.Client, func()) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Cmd:          []string{"-jar", "DynamoDBLocal.jar", "-sharedDb"}, // to be able to inspect in NoSQL workbench
		Repository:   "amazon/dynamodb-local",
		Tag:          "latest",
		ExposedPorts: []string{"8000"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"8000/tcp": {{HostIP: "", HostPort: "8000"}},
		},
	}, func(hostConfig *docker.HostConfig) {
		hostConfig.AutoRemove = true
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			port := resource.GetPort("8000/tcp")
			return aws.Endpoint{URL: fmt.Sprintf("http://localhost:%s", port)}, nil
		})),
		config.WithRegion("us-west-2"))
	if err != nil {
		panic(err)
	}

	client := dynamodb.NewFromConfig(cfg)

	if err := pool.Retry(func() error {
		return buildTable(client, tablename)
	}); err != nil {
		log.Fatalf("Could not connect to database: %s", err)
	}

	return resource, client, func() {
		err := pool.Purge(resource)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func buildTable(client *dynamodb.Client, tablename string) error {
	var createTableInput dynamodb.CreateTableInput
	fileContents, err := os.ReadFile("schema.json")
	if err != nil {
		return err
	}
	if err = json.Unmarshal(fileContents, &createTableInput); err != nil {
		return err
	}
	createTableInput.TableName = aws.String(tablename)
	_, err = client.CreateTable(context.Background(), &createTableInput)
	return err
}
