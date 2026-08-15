// Package filehandler takes care of basic file management for the containers, not restricted to a specific format
package filehandler

import (
	"encoding/json"
	"os"
)

// ReadJSON will take a struct or map and populate it with data in that format
func (f *File) ReadJSON(format any) error {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	data, err := os.ReadFile(f.Name)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, &format)
	if err != nil {
		return err
	}
	return nil
}

// WriteJSON takes any structured value and writes it to the file
func (f *File) WriteJSON(format any) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	data, err := json.Marshal(format)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(f.Name, os.O_WRONLY|os.O_TRUNC, os.FileMode(f.Permissions))
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			return //Likely already closed... whoops
		}
	}(file)

	if _, err := file.Write(data); err != nil {
		return err
	}
	return nil
}
