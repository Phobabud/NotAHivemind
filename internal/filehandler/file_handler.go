// Package filehandler takes care of basic file management for the containers, not restricted to a specific format
package filehandler

import (
	"errors"
	"os"
	"sync"
)

type File struct {
	Name        string
	Permissions int

	mutex  sync.RWMutex
	logger *LogWriter
}

// SetUpFile constructs a custom file object that handles locking and safety itself.
func SetUpFile(fileName string, permissions int) (File, error) {
	if fileName == "" {
		return File{}, errors.New("file name cannot be empty")
	}

	// Check if file exists, if not create it with the provided permissions
	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, os.FileMode(permissions))
		if err != nil {
			return File{}, err
		}
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				return
			}
		}(file)
	} else if err != nil {
		return File{}, err
	}

	return File{
		Name:        fileName,
		Permissions: permissions,
		mutex:       sync.RWMutex{},
	}, nil
}

// ReadLines will return a byte array of the lines read from the file
func (f *File) ReadLines() ([]byte, error) {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	data, err := os.ReadFile(f.Name)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// WriteLines will write an array of strings to a file
func (f *File) WriteLines(lines []string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	file, err := os.OpenFile(f.Name, os.O_WRONLY|os.O_TRUNC, os.FileMode(f.Permissions))
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			return
		}
	}(file)

	for _, line := range lines {
		if _, err := file.WriteString(line + "\n"); err != nil {
			return err
		}
	}
	return nil
}

// AppendLines will add an array of strings to the end of a file
func (f *File) AppendLines(lines []string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	file, err := os.OpenFile(f.Name, os.O_WRONLY|os.O_APPEND, os.FileMode(f.Permissions))
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			return
		}
	}(file)

	for _, line := range lines {
		if _, err := file.WriteString(line + "\n"); err != nil {
			return err
		}
	}
	return nil
}

// Delete will remove the file from the system, and will point the struct to empty strings and 0 permissions.
func (f *File) Delete() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	err := os.Remove(f.Name)
	if err != nil {
		return err
	}

	f.Name = ""
	f.Permissions = 0
	return nil
}
