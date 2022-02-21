// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/simagix/gox"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	// CacheIndexFileExt is .index
	CacheIndexFileExt = ".index"
	// GZippedBSONFileExt is .bson.gz
	GZippedBSONFileExt = ".bson.gz"

	// MetaDBName defines default meta database name
	MetaDBName = "_neutrino"
	// MetaLogCollection defines default meta oplogs collection name
	MetaLogCollection = "logs"
	// MetaOplogCollection defines default meta oplogs collection name
	MetaOplogCollection = "oplogs"
	// MetaTaskCollection defines default meta tasks collection name
	MetaTaskCollection = "tasks"
)

// Workspace stores meta database
type Workspace struct {
	db        *mongo.Database
	workspace string
}

// DropMetaDB drops meta database
func (p *Workspace) DropMetaDB() error {
	if p.db == nil {
		return fmt.Errorf("db %v is nil", MetaDBName)
	}
	return p.db.Drop(context.Background())
}

// CleanUpWorkspace removes all cached file
func (p *Workspace) CleanUpWorkspace() error {
	if p.workspace == "" {
		return fmt.Errorf("workspace is not defined")
	}
	var err error
	var filenames []string
	filepath.WalkDir(p.workspace, func(s string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(d.Name()) == CacheIndexFileExt {
			filenames = append(filenames, s)
		}
		return nil
	})
	for _, filename := range filenames {
		var reader *bufio.Reader
		if reader, err = gox.NewFileReader(filename); err != nil {
			return err
		}
		for {
			var buf []byte
			if buf, _, err = reader.ReadLine(); err != nil { // 0x0A separator = newline
				break
			}
			if err = os.Remove(string(buf)); err != nil {
				return err
			}
		}
		if err = os.Remove(filename); err != nil {
			return err
		}
	}
	return nil
}

// Reset drops meta database and clean up workspace
func (p *Workspace) Reset() error {
	var err error
	if err = p.DropMetaDB(); err != nil {
		return err
	}
	return p.CleanUpWorkspace()
}
