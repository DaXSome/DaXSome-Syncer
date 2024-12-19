package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/daxsome/daxsome-syncer/database"
	"github.com/daxsome/daxsome-syncer/storage"
	"github.com/daxsome/daxsome-syncer/utils"
	"github.com/joho/godotenv"
)

func readFromCSV(filepath string) ([][]string, error) {
	var records [][]string

	file, err := os.Open(filepath)
	if err != nil {
		return records, fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return records, fmt.Errorf("error reading CSV: %v", err)
		}
		records = append(records, record)
	}

	return records, nil
}

func convertToCSV(data []map[string]interface{}, outputFile string, isNew bool) error {
	var file *os.File

	file, err := os.OpenFile(outputFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error creating file: %v", err)
	}

	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	var headers []string

	if isNew {
		headers = extractHeaders(data)
		if err := writer.Write(headers); err != nil {
			return fmt.Errorf("error writing headers: %v", err)
		}
	} else {
		reader := csv.NewReader(file)

		for i := 0; i < 1; i++ {
			headers, _ = reader.Read()
		}
	}

	for _, row := range data {
		csvRow := make([]string, len(headers))
		for j, header := range headers {
			value := row[header]

			if value != nil && reflect.TypeOf(value).Kind() == reflect.String {
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
		headers = append(headers, strings.ToLower(k))
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
			defer wg.Done()

			filename := fmt.Sprintf("%v-%v.csv", data.Database, data.Collection)

			fullPath := filepath.Join("data", filename)

			var lastID string
			isNew := true

			if _, err := os.Stat(fullPath); err == nil {

				records, err := readFromCSV(fullPath)
				if err != nil {
					utils.Logger("error", err)
					return
				}

				slices.Reverse(records)

				lastID = records[0][0]
				isNew = false

			}

			docs, err := db.GetData(data, lastID)
			if err != nil {
				utils.Logger("error", err)
				return
			}

			if len(docs) > 0 {

				convertToCSV(docs, fullPath, isNew)

				url, err := storage.UploadFile(context.TODO(), fullPath)
				if err != nil {
					utils.Logger("error", err)
					return
				}

				log.Println(url)
			}
		}(data)
	}

	wg.Wait()
}
