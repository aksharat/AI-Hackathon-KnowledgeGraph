package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/hypermodeinc/modus/sdk/go" // Keeping this import as is
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func main() {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	// Fetch Neo4j connection details from environment variables
	dbUri := os.Getenv("NEO4J_URI")
	dbUser := os.Getenv("NEO4J_USERNAME")
	dbPassword := os.Getenv("NEO4J_PASSWORD")

	// Validate required environment variables
	if dbUri == "" || dbUser == "" || dbPassword == "" {
		log.Fatalf("Missing required environment variables: NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD")
	}

	// Initialize Neo4j driver
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(dbUri, neo4j.BasicAuth(dbUser, dbPassword, ""))
	if err != nil {
		log.Fatalf("Failed to create Neo4j driver: %v", err)
	}
	defer driver.Close(ctx)

	// Verify connectivity to the database
	err = driver.VerifyConnectivity(ctx)
	if err != nil {
		log.Fatalf("Failed to verify connectivity to Neo4j: %v", err)
	}
	fmt.Println("Connected to Neo4j successfully!")

	// Path to the CSV file
	csvFilePath := "urban_planning_data.csv"

	// Initialize the database with data from the CSV
	err = initializeDatabase(ctx, driver, csvFilePath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	// Prompt the user for input
	fmt.Println("Enter your query (e.g., 'I am moving to AZ with 3 kids, and I work at Intel. Suggest me good places to live'):")

	// // Declare a variable to hold the input
	// var userQuery string

	// // Read the input from the user
	// fmt.Scanln(&userQuery)

	// // Output the input
	// fmt.Println("You entered:", userQuery)

	// Query the database and display an example output
	question := "What are the utilities serving buildings in Scottsdale?"
	graphData, err := queryGraph(ctx, driver, question)
	if err != nil {
		log.Fatalf("Error querying graph: %v", err)
	}

	// Generate an AI-like response using the queried data
	finalResponse := generateAIResponse(question, graphData)
	fmt.Println("\nFinal Response from AI:")
	fmt.Println(finalResponse)
}

// queryGraph queries the Neo4j database based on a specific question.
func queryGraph(ctx context.Context, driver neo4j.DriverWithContext, question string) (string, error) {
	// Cypher query to find buildings and utilities in Scottsdale
	cypherQuery := `
		MATCH (b:Building)-[:WITHIN_ZONE]->(z:Zone {name: 'Scottsdale'})-[:SERVED_BY]->(u:Utility)
		RETURN b.name AS building, u.name AS utility
	`

	fmt.Println("Running Cypher Query:")
	fmt.Println(cypherQuery)

	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	data := []map[string]interface{}{}
	_, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, err := tx.Run(ctx, cypherQuery, nil)
		if err != nil {
			return nil, err
		}

		for result.Next(ctx) {
			record := result.Record()
			building, _ := record.Get("building")
			utility, _ := record.Get("utility")

			fmt.Printf("Found record: building=%v, utility=%v\n", building, utility)

			data = append(data, map[string]interface{}{
				"building": building,
				"utility":  utility,
			})
		}

		if len(data) == 0 {
			fmt.Println("No results found for the query.")
		}

		return nil, result.Err()
	})
	if err != nil {
		return "", err
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	return string(jsonData), nil
}

// // generateAIResponse generates an AI-like response using the graph data.
func generateAIResponse(question, graphData string) string {
	// Simulate AI processing by combining the question and data
	response := fmt.Sprintf(
		"Question: %s\nGraph Data:",
		question,
		graphData,
	)

	return response
}
