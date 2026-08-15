package filesystem

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadStorage(t *testing.T) {
	tests := []struct {
		name             string
		setupSandbox     func(t *testing.T, dataDir string) // Prepares the temp dir with mock files
		expectedTerm     int64
		expectedIndex    int64
		expectError      bool
		expectedFileName string
	}{
		{
			name: "Brand new cluster (no files)",
			setupSandbox: func(t *testing.T, dataDir string) {
				// We do nothing. The directory is empty.
			},
			expectedTerm:     0,
			expectedIndex:    0,
			expectError:      false,
			expectedFileName: "0-0.log",
		},
		{
			name: "Existing snapshot but no logs",
			setupSandbox: func(t *testing.T, dataDir string) {
				// Should fall back to 0-0.log
				file, err := os.Create(filepath.Join(dataDir, "active.log"))
				if err != nil {
					t.Fatalf("Failed to setup sandbox: %v", err)
				}
				file.Close()
			},
			expectedTerm:     0,
			expectedIndex:    0,
			expectError:      false,
			expectedFileName: "0-0.log",
		},
		{
			name: "Existing log files without snapshot",
			setupSandbox: func(t *testing.T, dataDir string) {
				// Create some stale logs
				createMockFile(t, dataDir, "1-10.log")
				createMockFile(t, dataDir, "1-50.log")

				// Create the "latest" log
				createMockFile(t, dataDir, "2-100.log")
			},
			expectedTerm:     2,
			expectedIndex:    100,
			expectError:      false,
			expectedFileName: "2-100.log", // It should reopen the latest log for appending
		},
		{
			name: "Malformed log files are ignored",
			setupSandbox: func(t *testing.T, dataDir string) {
				createMockFile(t, dataDir, "not-a-log.txt")
				createMockFile(t, dataDir, "broken-name.log")
				createMockFile(t, dataDir, "3-45.log") // This should be the one it picks
			},
			expectedTerm:     3,
			expectedIndex:    45,
			expectError:      false,
			expectedFileName: "3-45.log",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir := t.TempDir()

			tt.setupSandbox(t, dataDir)

			handler, err := LoadStorage(dataDir)

			if (err != nil) != tt.expectError {
				t.Fatalf("LoadStorage() error = %v, expectError = %v", err, tt.expectError)
			}

			if err != nil {
				return // If we expected an error, there's nothing else to check
			}

			if handler == nil {
				t.Fatalf("LoadStorage() returned a nil Handler without an error")
			}

			// Need to clean up before we move on (windows lmao)
			t.Cleanup(func() {
				if handler.activeFile != nil {
					handler.activeFile.Close()
				}
				if handler.snapshotFile != nil {
					handler.snapshotFile.Close()
				}
			})

			if handler.discTerm != tt.expectedTerm {
				t.Errorf("Expected discTerm %d, got %d", tt.expectedTerm, handler.discTerm)
			}

			if handler.discIndex != tt.expectedIndex {
				t.Errorf("Expected discIndex %d, got %d", tt.expectedIndex, handler.discIndex)
			}

			// Check if the file it opened/created matches what we expect
			activeBaseName := filepath.Base(handler.activeFile.Name())
			if activeBaseName != tt.expectedFileName {
				t.Errorf("Expected active file %s, got %s", tt.expectedFileName, activeBaseName)
			}

			if handler.activeFile != nil {
				handler.activeFile.Close()
			}
			if handler.snapshotFile != nil {
				handler.snapshotFile.Close()
			}
		})
	}
}

// createMockFile is a test helper to quickly generate dummy files in the sandbox
func createMockFile(t *testing.T, dir string, name string) {
	t.Helper() // Tells Go test runner to report errors here
	path := filepath.Join(dir, name)
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("Failed to create mock file %s: %v", name, err)
	}
	file.Close()
}
