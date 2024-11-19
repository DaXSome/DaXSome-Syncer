package database

import (
	"context"
	"fmt"
	"os"

	"github.com/daxsome/daxsome-syncer/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Database struct {
	*mongo.Client
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
		Client: client,
	}
}

func (client *Database) GetDatasets() ([]Dataset, error) {
	utils.Logger("database", "[+] Getting datasets...")

	db := client.Database("datasets")

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

	utils.Logger("database", fmt.Sprintf("[+] Got %v datasets", len(datasets)))

	return datasets, nil
}

func (client *Database) GetData(dataset Dataset) ([]map[string]interface{}, error) {
	utils.Logger("database", fmt.Sprintf("[+] Getting data from %v.%v", dataset.Database, dataset.Collection))

	opts := options.FindOptions{}
	opts.SetLimit(10)

	data := []map[string]interface{}{}

	docs, err := client.Database(dataset.Database).Collection(dataset.Collection).Find(context.TODO(), bson.D{}, &opts)
	if err != nil {
		return data, err
	}

	for docs.Next(context.TODO()) {
		result := make(map[string]interface{})

		docs.Decode(&result)

		data = append(data, result)

	}

	utils.Logger("database", fmt.Sprintf("[+] Got %v documents from %v.%v", len(data), dataset.Database, dataset.Collection))

	return data, nil
}
