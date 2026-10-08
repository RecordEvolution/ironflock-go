// Stores files in the app's managed object storage and links them from a
// table row.
package main

import (
	"context"
	"errors"
	"log"
	"os"

	ironflock "github.com/RecordEvolution/ironflock-go"
	"github.com/RecordEvolution/ironflock-go/filestore"
)

func main() {
	ifl, err := ironflock.New()
	if err != nil {
		log.Fatal(err)
	}
	err = ifl.Run(context.Background(), func(ctx context.Context) error {
		files := ifl.Files()

		// Store an object; the URL is permanent and safe to keep in a column.
		info, err := files.Put(ctx, "inspections/part-1.txt", []byte("all good"),
			filestore.ContentType("text/plain"))
		var ferr *filestore.Error
		if errors.As(err, &ferr) && ferr.Code == filestore.CodeQuotaExceeded {
			log.Print("file store is full")
			return nil
		} else if err != nil {
			return err
		}
		if err := ifl.PublishToTable(ctx, "inspections", ironflock.Row{"part_id": "1", "report_url": info.URL}); err != nil {
			return err
		}

		// Large files stream straight to the object store.
		if path := os.Getenv("UPLOAD_FILE"); path != "" {
			if _, err := files.PutFile(ctx, "uploads/"+info.Key, path); err != nil {
				return err
			}
		}

		data, err := files.Get(ctx, "inspections/part-1.txt")
		if err != nil {
			return err
		}
		log.Printf("read back %q", data)

		for obj, err := range files.Iter(ctx, filestore.Prefix("inspections/")) {
			if err != nil {
				return err
			}
			log.Printf("%s (%d bytes) %s", obj.Key, obj.Size, obj.URL)
		}

		usage, err := files.Usage(ctx)
		if err != nil {
			return err
		}
		log.Printf("%d bytes in %d objects, %d bytes free", usage.SizeBytes, usage.ObjectCount, usage.FreeBytes)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
