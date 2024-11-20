package database

import (
	"context"
	"fmt"
	"os"
	"time"

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

func (client *Database) GetData(dataset Dataset, snapshot map[string]time.Time) ([]map[string]interface{}, error) {
	utils.Logger("database", fmt.Sprintf("[+] Checking last update for %v.%v", dataset.Database, dataset.Collection))

	opts := options.FindOptions{}

	data := []map[string]interface{}{}

	db := client.Database(dataset.Database)

	metadataDoc := db.Collection("meta_data").FindOne(context.TODO(), bson.D{})

	metadata := struct {
		UpdatedAt string `bson:"updated_at"`
	}{}

	metadataDoc.Decode(&metadata)

	parsedTime, err := time.Parse("2006-01-02T15:04:05Z", metadata.UpdatedAt)
	if err != nil {
		return data, err
	}

	isOutdated := parsedTime.After(snapshot[dataset.Database])

	if isOutdated {
		utils.Logger("database", fmt.Sprintf("[+] Getting data from %v.%v", dataset.Database, dataset.Collection))

		docs, err := db.Collection(dataset.Collection).Find(context.TODO(), bson.D{}, &opts)
		if err != nil {
			return data, err
		}

		for docs.Next(context.TODO()) {
			result := make(map[string]interface{})

			docs.Decode(&result)

			data = append(data, result)

		}

		utils.Logger("database", fmt.Sprintf("[+] Got %v documents from %v.%v", len(data), dataset.Database, dataset.Collection))

		snapshot[dataset.Database] = parsedTime

		return data, nil

	}

	utils.Logger("database", fmt.Sprintf("[+] %v-%v is up to date", dataset.Database, dataset.Collection))

	return data, nil
}
