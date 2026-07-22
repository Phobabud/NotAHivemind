// Package filehandler handles basic file management. This file contains functions to handle groups of files.
package filehandler

//This is meant for managing multiple files, in my implementation of pointer hell

type FileCluster map[string]*File

func (files *FileCluster) ContainsFile(fileName string) bool {
	if fileName == "" {
		return false
	}

	_, exists := (*files)[fileName]
	return exists
}

func (files *FileCluster) GetFile(fileName string) *File {
	if files.ContainsFile(fileName) {
		return (*files)[fileName]
	}
	return nil
}

func (files *FileCluster) AddFile(file *File) {
	if !files.ContainsFile(file.Name) {
		(*files)[file.Name] = file
	}
}

func (files *FileCluster) RemoveFile(fileName string) {
	if files.ContainsFile(fileName) {
		delete(*files, fileName)
	}
}
