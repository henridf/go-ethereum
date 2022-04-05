// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rawdb

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/log"
)

// copyFrom copies data from 'srcPath' at offset 'offset' into 'destPath'.
// The 'destPath' is created if it doesn't exist, otherwise it is overwritten.
// Before the copy is executed, there is a callback can be registered to
// manipulate the dest file.
// It is perfectly valid to have destPath == srcPath.
func copyFrom(srcPath, destPath string, offset uint64, before func(f *os.File) error) error {
	// Create a temp file in the same dir where we want it to wind up
	f, err := ioutil.TempFile(filepath.Dir(destPath), "*")
	if err != nil {
		return err
	}
	fname := f.Name()

	// Clean up the leftover file
	defer func() {
		if f != nil {
			f.Close()
		}
		os.Remove(fname)
	}()
	// Apply the given function if it's not nil before we copy
	// the content from the src.
	if before != nil {
		if err := before(f); err != nil {
			return err
		}
	}
	// Open the source file
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	if _, err = src.Seek(int64(offset), 0); err != nil {
		src.Close()
		return err
	}
	// io.Copy uses 32K buffer internally.
	_, err = io.Copy(f, src)
	if err != nil {
		src.Close()
		return err
	}
	// Rename the temporary file to the specified dest name.
	// src may be same as dest, so needs to be closed before
	// we do the final move.
	src.Close()

	if err := f.Close(); err != nil {
		return err
	}
	f = nil

	if err := os.Rename(fname, destPath); err != nil {
		return err
	}
	return nil
}

// openFreezerFileForAppend opens a freezer table file and seeks to the end
func openFreezerFileForAppend(filename string) (*os.File, error) {
	// Open the file without the O_APPEND flag
	// because it has differing behaviour during Truncate operations
	// on different OS's
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	// Seek to end for append
	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	return file, nil
}

// openFreezerFileForReadOnly opens a freezer table file for read only access
func openFreezerFileForReadOnly(filename string) (*os.File, error) {
	return os.OpenFile(filename, os.O_RDONLY, 0644)
}

// openFreezerFileTruncated opens a freezer table making sure it is truncated
func openFreezerFileTruncated(filename string) (*os.File, error) {
	return os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
}

// truncateFreezerFile resizes a freezer table file and seeks to the end
func truncateFreezerFile(file *os.File, size int64) error {
	if err := file.Truncate(size); err != nil {
		return err
	}
	// Seek to end for append
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

func Concat(to, from *freezer) error {
	for name, totab := range to.tables {
		log.Debug("backfilling ancients", "table", name)
		fromtab, ok := from.tables[name]
		if !ok {
			return fmt.Errorf("table %s not in source freezer", name)
		}
		err := concat(totab, fromtab)
		if err != nil {
			return fmt.Errorf("concatenating tables %s: %s", name, err)
		}
	}

	toPath := to.tables["headers"].path
	fromPath := from.tables["headers"].path

	log.Debug("moving ancient dir", "from", fromPath, "to", fromPath+".old")
	os.Rename(fromPath, fromPath+".old")
	log.Debug("moving ancient dir", "from", toPath, "to", fromPath)
	os.Rename(toPath, fromPath)

	return nil
}

func readIndex(t *freezerTable, i uint64) (*indexEntry, error) {
	buffer := make([]byte, indexEntrySize)
	if _, err := t.index.ReadAt(buffer, int64(i*indexEntrySize)); err != nil {
		return nil, err
	}
	entry := new(indexEntry)
	entry.unmarshalBinary(buffer)
	return entry, nil
}

func concat(to, from *freezerTable) error {
	index, err := openFreezerFileForAppend(to.index.Name())
	if err != nil {
		return err
	}
	toIndex := bufio.NewWriter(index)
	toFileId := to.headId + 1
	fromFileId := from.tailId

	cur := uint64(0)
	for {
		entry, err := readIndex(from, cur)
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}
		if entry.filenum != fromFileId {
			toFileId++
			log.Debug("Completed entries for file, renaming and starting entries for new file",
				"completed", from.fileName(fromFileId), "rename_to", to.fileName(toFileId), "new", from.fileName(fromFileId+1))
			if err := os.Rename(from.fileName(fromFileId), to.fileName(toFileId)); err != nil {
				return err
			}
			if fromFileId != entry.filenum+1 {
				return fmt.Errorf("unexpected jump from %d to %d", entry.filenum, fromFileId)
			}
			fromFileId = entry.filenum
		}
		entry.filenum = toFileId
		if _, err := toIndex.Write(entry.append(nil)); err != nil {
			return fmt.Errorf("error index: %e\n", err)
		}
		cur++
	}
	if err := toIndex.Flush(); err != nil {
		return err
	}
	if err := index.Close(); err != nil {
		return err
	}

	log.Debug("Completed index, renaming data file", "from", from.fileName(fromFileId), "to", to.fileName(toFileId))
	if err := os.Rename(from.fileName(fromFileId), to.fileName(toFileId)); err != nil {
		return err
	}
	return nil
}
