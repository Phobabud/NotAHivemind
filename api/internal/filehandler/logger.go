// Package filehandler takes care of basic file management. This file takes care of logging needs from cluster stdout.
package filehandler

import (
	"bufio"
	"os"
	"path"
	"sync"

	"github.com/golang/glog"
)

// LogWriter Designed to write data from the containers to separate files, keeping container data and main data apart
type LogWriter struct {
	folderPath  string
	permissions os.FileMode
	dataQueue   chan Log
	files       sync.Map // map[string]*os.File
}

type Log struct {
	FileName string
	Entries  []string
}

// NewLogWriter initializes a new LogWriter with the specified log folder path, file permissions, and queue size for entries.
func NewLogWriter(logFolderPath string, permissions os.FileMode, queueSize int) (LogWriter, error) {
	err := os.MkdirAll(logFolderPath, os.ModePerm)
	if err != nil {
		return LogWriter{}, err
	}

	return LogWriter{
		folderPath:  logFolderPath,
		permissions: permissions,
		dataQueue:   make(chan Log, queueSize),
		files:       sync.Map{},
	}, nil
}

// Add takes the load off of the goroutine, allow it to end without the workload of managing logging itself (is this smart?)
func (w *LogWriter) Add(entry []string, filename string) {
	if _, ok := w.files.Load(filename); !ok {
		filePath := path.Join(w.folderPath, filename)
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, w.permissions)
		if err != nil {
			glog.Errorf("Failed to open file %s for append: %v", filename, err)
			return // Skip adding this log entry
		}
		w.files.Store(filename, file)
	}

	w.dataQueue <- Log{
		FileName: filename,
		Entries:  entry,
	}
}

// FreeFile frees the file from the handler, helpful for files used temporarily and not accessed often
func (w *LogWriter) FreeFile(filename string) {
	if file, ok := w.files.Load(filename); ok {
		if f, ok := file.(*os.File); ok {
			err := f.Close()
			if err != nil {
				glog.Errorf("Failed to close file %s: %v", filename, err)
			}
		}
		w.files.Delete(filename)
	}
}

// Close frees all the files in the handler and clears everything, including shutting down the logger
func (w *LogWriter) Close() {
	close(w.dataQueue)
	w.files.Range(func(key, value interface{}) bool {
		if file, ok := value.(*os.File); ok {
			err := file.Close()
			if err != nil {
				glog.Errorf("Failed to close file: %v", err)
			}
		}
		return true
	})
}

// AsyncWrite Meant to be run as a goroutine to constantly check if it's running. Terminates when channel closes
//
// Considering that this primarily runs to dump logs from the containers
func (w *LogWriter) AsyncWrite() {
	//Write to file every time the channel has an entry
	for val := range w.dataQueue {
		filePath := w.folderPath + val.FileName
		if _, ok := w.files.Load(val.FileName); !ok {
			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, w.permissions)
			if err != nil {
				glog.Errorf("Failed to find file %s for append in logger: %v", val.FileName, err)
			}
			w.files.Store(val.FileName, file)
			glog.Infof("Created new log file: %s", val.FileName)
		}

		file, _ := w.files.Load(val.FileName)
		writer := bufio.NewWriter(file.(*os.File))

		for _, line := range val.Entries {
			// Write line and newline to the buffered writer
			if _, err := writer.WriteString(line + "\n"); err != nil {
				glog.Errorf("Failed to write line to buffer for file %s: %v", val.FileName, err)
				break // Stop writing this entry on error
			}
		}

		//Flush the buffer
		if err := writer.Flush(); err != nil {
			glog.Errorf("Failed to flush writer to file %s: %v", val.FileName, err)
		}
	}
}
