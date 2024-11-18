package database

import (
	"context"
	"os"

	"github.com/daxsome/daxsome-syncer/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Database struct {
	*mongo.Database
}

type Dataset struct {
	Database   string `bson:"database"`
	Collection string `bson:"sample_collection"`
}

// NewDatabase initializes a new instance of the Database struct.
//
// Returns a pointer to the newly created Database.
func NewDatabase() *Database {
	utils.Logger("database", "[+] Initing database...")

	dbURI := os.Getenv("DB_URI")

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(dbURI))
	if err != nil {
		utils.Logger("error", err)
	}

	utils.Logger("database", "[+] Database initialized!")

	return &Database{
		Database: client.Database("datasets"),
	}
}

func (db *Database) GetDatasets() ([]Dataset, error) {
	docs, err := db.Collection("datasets").Find(context.TODO(), bson.M{})
	if err != nil {
		return []Dataset{}, err
	}

	datasets := []Dataset{}

	for docs.Next(context.TODO()) {
		result := Dataset{}

		docs.Decode(&result)

		datasets = append(datasets, result)

	}

	return datasets, nil
}
