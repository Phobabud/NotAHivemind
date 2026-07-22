package environment

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// ImportDirectory is a generic importer for recursively importing any file it finds in a directory and subdirectory into a type. File must be a json file
func ImportDirectory[T any](path string, exclude []string) ([]*T, error) {
	files, err := GetAllFiles(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", path, err)
	}

	// If any of the files have a name or part of a name that matches exclude, skip it
	var dataList []*T
	for _, file := range files {
		if filepath.Ext(file) != ".json" {
			continue
		}

		// If it matches anything from the exclusion list, move on
		shouldExclude := false
		for _, substr := range exclude {
			if strings.Contains(file, substr) {
				shouldExclude = true
				break
			}
		}
		if shouldExclude {
			continue
		}

		// File passed checks, so we can import it
		data, err := ImportFile[T](file)
		if err != nil {
			return nil, err
		}
		dataList = append(dataList, data)
	}

	return dataList, nil
}

// ImportFile is a generic importer for any json file
func ImportFile[T any](path string) (*T, error) {
	fileBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}
	var t T
	if err := json.Unmarshal(fileBytes, &t); err != nil {
		return nil, fmt.Errorf("failed to parse JSON from %s: %w", path, err)
	}

	return &t, nil
}

// GetAllFiles returns a list of all file paths under the specified root directory.
func GetAllFiles(rootDir string) ([]string, error) {
	var files []string

	err := filepath.WalkDir(rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			files = append(files, path)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}
