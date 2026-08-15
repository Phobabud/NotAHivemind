# Supposed to be a kinda bad program that doesn't survive after a single job to help test under load
# (probably should be a good idea to write it in go if this needed to be done well)
import os
import glob
import json
import sys
import time

# These paths map exactly to the 'Target: "/job/"' bind mount in container.go
PAYLOAD_DIR = "/job/payload"
RESULTS_DIR = "/job/results"

def wait_for_file(directory, pattern, timeout=10):
    """Polls the directory until a file matching the pattern exists and is stable."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        files = glob.glob(os.path.join(directory, pattern))
        if files:
            try:
                # Poll until the file is readable (ensures atomic move completed)
                with open(files[0], 'r') as f:
                    json.load(f)
                return files[0]
            except (json.JSONDecodeError, IOError):
                pass
        time.sleep(0.05)
    return None

def main():
    # 1. Wait for payload to be atomically available
    payload_file = wait_for_file(PAYLOAD_DIR, "*.json")
    if not payload_file:
        sys.exit(1)

    filename = os.path.basename(payload_file)
    tmp_result_file = os.path.join(RESULTS_DIR, filename + ".tmp")
    final_result_file = os.path.join(RESULTS_DIR, filename)

    try:
        with open(payload_file, 'r') as f:
            data = json.load(f)

        # 2. Atomic result write: Write to .tmp then rename
        with open(tmp_result_file, 'w') as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno()) # Ensure data hits physical disk

        # os.rename is atomic on Linux/Unix/Docker
        os.rename(tmp_result_file, final_result_file)

    except Exception as e:
        sys.exit(1)

    sys.exit(0)

if __name__ == "__main__":
    main()