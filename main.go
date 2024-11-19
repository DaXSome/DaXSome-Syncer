package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/daxsome/daxsome-syncer/database"
	"github.com/daxsome/daxsome-syncer/storage"
	"github.com/daxsome/daxsome-syncer/utils"
	"github.com/joho/godotenv"
)

func convertToCSV(data []map[string]interface{}, outputFile string) error {
	headers := extractHeaders(data)

	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("error creating file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("error writing headers: %v", err)
	}

	for _, row := range data {
		csvRow := make([]string, len(headers))
		for j, header := range headers {
			value := row[header]

			if reflect.TypeOf(value).Kind() == reflect.String {
				value = strings.ReplaceAll(value.(string), "&amp;", "&") // Prevent ';' being treated as separator
			}

			csvRow[j] = fmt.Sprintf("%v", value)
		}
		if err := writer.Write(csvRow); err != nil {
			return fmt.Errorf("error writing row: %v", err)
		}

	}

	return nil
}

func extractHeaders(data []map[string]interface{}) []string {
	headers := []string{}

	for k := range data[0] {
		// Skip doc id
		if k == "_id" {
			continue
		}
		headers = append(headers, k)
	}
	sort.Strings(headers)

	return headers
}

func main() {
	godotenv.Load()

	db := database.NewDatabase()

	storage, err := storage.NewStorage()
	utils.HandleErr(err, "failed to setup storage")

	wg := sync.WaitGroup{}

	datasets, err := db.GetDatasets()
	utils.HandleErr(err, "failed to get datasets")

	for _, data := range datasets {
		wg.Add(1)
		go func(data database.Dataset) {
			docs, err := db.GetData(data)
			if err != nil {
				utils.Logger("error", err)
				return
			}

			filename := fmt.Sprintf("%v-%v.csv", data.Database, data.Collection)
			convertToCSV(docs, filename)

			url, err := storage.UploadFile(context.TODO(), filename)
			if err != nil {
				utils.Logger("error", err)
				return
			}

			log.Println(url)
			wg.Done()
		}(data)
	}

	wg.Wait()
}
