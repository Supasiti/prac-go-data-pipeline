package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	src "github.com/Supasiti/prac-go-data-pipeline/internal/models/source"
	faker "github.com/brianvoe/gofakeit/v7"
)

const (
	count        = 10
	fileTemplate = "tests/data/source_%v.txt"
)

func generateSource() (*src.Source, error) {
	postfixOpts := []any{"", faker.NameSuffix()}
	postfix, err := faker.Weighted(postfixOpts, []float32{0.4, 0.6})
	if err != nil {
		return nil, err
	}

	r := &src.Source{
		Id:          strconv.Itoa(faker.Number(100000, 999999)),
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
	filename := fmt.Sprintf(fileTemplate, count)
	log.Printf("[INFO] Generating source data to file: %s\n", filename)

	// open file
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("[ERROR] Failed to open file %s for writing: %v", filename, err)
	}
	defer file.Close()

	// write each row without comma
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "")
	for i := range count {
		s, err := generateSource()
		if err != nil {
			log.Fatalf("Failed to generate source: %v", err)
		}

		if err := encoder.Encode(s); err != nil {
			log.Fatalf("Failed to encode source %d to JSON: %v", i, err)
		}
	}

	log.Printf("[INFO] Successfully generated %d rows to file: %s\n", count, filename)

}
