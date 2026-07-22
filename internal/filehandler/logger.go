// Package filehandler takes care of basic file management. This file takes care of logging needs from cluster stdout.
package filehandler

import (
	"bufio"
	"log"
	"os"
)

// LogWriter Designed to write data from the containers to separate files, keeping container data and main data apart
type LogWriter struct {
	folderPath  string
	permissions os.FileMode
	dataQueue chan Log
	files     map[string]*os.File
}

type Log struct {
	FileName string
	Entries  []string
}

// NewLogWriter initializes a new LogWriter with the specified log folder path, file permissions, and queue size for entries.
func NewLogWriter(logFolderPath string, permissions os.FileMode, queueSize int) LogWriter {
	return LogWriter{
		folderPath:  logFolderPath,
		permissions: permissions,
		dataQueue:   make(chan Log, queueSize),
		files:       make(map[string]*os.File),
	}
}

// Add takes the load off of the goroutine, allow it to end without the workload of managing logging itself (is this smart?)
func (w *LogWriter) Add(entry []string, filename string) {
	if w.files[filename] == nil {
		filePath := w.folderPath + filename
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, w.permissions)
		if err != nil {
			log.Printf("Failed to open file %s for append: %v", filename, err)
			return // Skip adding this log entry
		}
		w.files[filename] = file
	}

	w.dataQueue <- Log{
		FileName: filename,
		Entries:  entry,
	}
}

// FreeFile frees the file from the handler, helpful for files used temporarily and not accessed often
func (w *LogWriter) FreeFile(filename string) {
	if w.files[filename] != nil {
		err := w.files[filename].Close()
		if err != nil {
			log.Printf("Failed to close file %s: %v", filename, err)
		}
		delete(w.files, filename)
	}
}

// Close frees all the files in the handler and clears everything, including shutting down the logger
func (w *LogWriter) Close() {
	close(w.dataQueue)
	for _, file := range w.files {
		err := file.Close()
		if err != nil {
			log.Printf("Failed to close file: %v", err)
		}
	}
}

// AsyncWrite Meant to be run as a goroutine to constantly check if it's running. Terminates when channel closes
//
// Considering that this primarily runs to dump logs from the containers
func (w *LogWriter) AsyncWrite() {
	//Write to file every time the channel has an entry
	for val := range w.dataQueue {
		filePath := w.folderPath + val.FileName
		if w.files[val.FileName] == nil {
			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, w.permissions)
			if err != nil {
				log.Printf("Failed to find file %s for append in logger: %v", val.FileName, err)
			}
			w.files[val.FileName] = file
			log.Printf("Created new log file: %s", val.FileName)
		}

		writer := bufio.NewWriter(w.files[val.FileName])

		for _, line := range val.Entries {
			// Write line and newline to the buffered writer
			if _, err := writer.WriteString(line + "\n"); err != nil {
				log.Printf("Failed to write line to buffer for file %s: %v", val.FileName, err)
				break // Stop writing this entry on error
			}
		}

		//Flush the buffer
		if err := writer.Flush(); err != nil {
			log.Printf("Failed to flush writer to file %s: %v", val.FileName, err)
		}
	}
}
