package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	firebase "firebase.google.com/go/v4"
	"github.com/daxsome/daxsome-syncer/utils"
	"google.golang.org/api/option"
)

type Storage struct {
	*storage.BucketHandle
}

func NewStorage() (*Storage, error) {
	config := &firebase.Config{
		StorageBucket: "exacheer-c9099.appspot.com",
	}
	opt := option.WithCredentialsFile("storage/sa.json")
	app, err := firebase.NewApp(context.Background(), config, opt)
	if err != nil {
		return nil, err
	}

	client, err := app.Storage(context.Background())
	if err != nil {
		return nil, err
	}

	bucket, err := client.DefaultBucket()
	if err != nil {
		return nil, err
	}

	return &Storage{bucket}, nil
}

func (s *Storage) UploadFile(ctx context.Context, file string) (string, error) {
	utils.Logger("storage", fmt.Sprintf("[+] Uploading file %v", file))

	f, err := os.Open(file)
	if err != nil {
		return "", fmt.Errorf("os.Open: %w", err)
	}

	defer f.Close()

	fullPath := filepath.Join("DaXSome", "datasets", filepath.Base(file))

	obj := s.Object(fullPath)

	writer := obj.NewWriter(ctx)
	defer writer.Close()

	writer.ContentType = "application/csv"

	if _, err := io.Copy(writer, f); err != nil {
		return "", fmt.Errorf("failed to copy file to storage: %v", err)
	}

	if err := obj.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
		return "", fmt.Errorf("failed to make file public: %v", err)
	}

	encodedPath := strings.ReplaceAll(fullPath, "/", "%2F")

	url := fmt.Sprintf("https://firebasestorage.googleapis.com/v0/b/exacheer-c9099.appspot.com/o/%s?alt=media", encodedPath)

	return url, nil
}
