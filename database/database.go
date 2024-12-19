package database

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/daxsome/daxsome-syncer/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

func (client *Database) GetLastID(dataset Dataset) string {
	db := client.Database(dataset.Database)

	doc := db.Collection(dataset.Collection).FindOne(context.TODO(), bson.D{})

	data := struct {
		ID string `bson:"_id"`
	}{}

	doc.Decode(&data)

	return data.ID
}

func (client *Database) GetData(dataset Dataset, lastID string) ([]map[string]interface{}, error) {
	utils.Logger("database", fmt.Sprintf("[+] Checking last update for %v.%v", dataset.Database, dataset.Collection))

	opts := options.FindOptions{}

	opts.SetSort(bson.D{{Key: "_id", Value: 1}})

	filter := bson.M{}

	data := []map[string]interface{}{}

	if lastID != "" {
		primitiveId, err := primitive.ObjectIDFromHex(lastID)
		if err != nil {
			return data, err
		}


		filter["_id"] = bson.M{"$gt": primitiveId}
	}

	db := client.Database(dataset.Database)

	utils.Logger("database", fmt.Sprintf("[+] Getting data from %v.%v", dataset.Database, dataset.Collection))

	docs, err := db.Collection(dataset.Collection).Find(context.TODO(), filter, &opts)
	if err != nil {
		return data, err
	}

	for docs.Next(context.TODO()) {
		result := make(map[string]interface{})

		docs.Decode(&result)

		if id, ok := result["_id"].(primitive.ObjectID); ok {
			result["_id"] = id.Hex()
		}

		// Convert all keys to lowercase
		for key, value := range result {
			newKey := strings.ToLower(key)
			result[newKey] = value

			if newKey != key {
				delete(result, key)
			}
		}

		data = append(data, result)

	}

	utils.Logger("database", fmt.Sprintf("[+] Got %v documents from %v.%v", len(data), dataset.Database, dataset.Collection))

	return data, nil
}
