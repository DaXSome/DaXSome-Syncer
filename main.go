package main

import (
	"log"

	"github.com/daxsome/daxsome-syncer/database"
	"github.com/daxsome/daxsome-syncer/utils"
	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()

	db := database.NewDatabase()

	datasets, err := db.GetDatasets()
	utils.HandleErr(err, "failed to get datasets")

	for _, data := range datasets {
		log.Println(data.Database, data.Collection)
	}
}
