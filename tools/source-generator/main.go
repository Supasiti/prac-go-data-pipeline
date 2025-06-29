package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Supasiti/prac-go-data-pipeline/internal/transformer"
	faker "github.com/brianvoe/gofakeit/v7"
)

const (
	count        = 1000000
	fileTemplate = "tests/data/source_%v.txt"
)

func generateSource(id int) (*transformer.Source, error) {
	postfixOpts := []any{"", faker.NameSuffix()}
	postfix, err := faker.Weighted(postfixOpts, []float32{0.4, 0.6})
	if err != nil {
		return nil, err
	}

	r := &transformer.Source{
		Id:          strconv.Itoa(id),
		Prefix:      faker.NamePrefix(),
		Postfix:     postfix.(string),
		FirstName:   faker.FirstName(),
		LastName:    faker.LastName(),
		MiddleName:  faker.MiddleName(),
		Gender:      faker.Gender(),
		DateOfBirth: faker.PastDate().Format(time.DateOnly),
	}
	return r, nil
}

func main() {
	start := time.Now()
	filename := fmt.Sprintf(fileTemplate, count)
	log.Printf("[INFO] Generating source data to file: %s\n", filename)

	// open file
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("[ERROR] Failed to open file %s for writing: %v", filename, err)
	}
	defer file.Close()

	progress := 0
	currentId := 10000000

	// write each row without comma
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "")

	for i := range count {
		currentId += faker.IntN(5) + 1
		s, err := generateSource(currentId)
		if err != nil {
			log.Fatalf("[ERROR] Failed to generate source: %v", err)
		}

		if err := encoder.Encode(s); err != nil {
			log.Fatalf("[ERROR] Failed to encode source %d to JSON: %v", i, err)
		}
		progress++

		if progress%10000 == 0 {
			log.Printf("[INFO] Generated %d rows\n", progress)
		}
	}

	log.Printf("[INFO] Successfully generated %d rows to file: %s\n", count, filename)
	log.Printf("[INFO] Total execution time: %v", time.Since(start))
}
