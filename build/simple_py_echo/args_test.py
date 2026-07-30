import os
import glob
import json
import sys
import time

# These paths map exactly to the 'Target: "/job/"' bind mount in container.go
PAYLOAD_DIR = "/job/payload"
RESULTS_DIR = "/job/results"

def main():
    print(f"Starting mirror task. Scanning {PAYLOAD_DIR}...")

    # Search for the injected job JSON file
    payload_files = glob.glob(os.path.join(PAYLOAD_DIR, "*.json"))

    if not payload_files:
        print(f"CRITICAL ERROR: No JSON files found in {PAYLOAD_DIR}")
        sys.exit(1) # Exit Code 1 will trigger a FAILED event in your Go scheduler

    for payload_file in payload_files:
        filename = os.path.basename(payload_file)
        result_file = os.path.join(RESULTS_DIR, filename)

        print(f"Reading payload from {filename}...")

        try:
            # Parse it to prove we successfully read the data
            with open(payload_file, 'r') as f:
                data = json.load(f)

            print(f"Payload successfully parsed: {data}")

            # Mirror the exact same JSON into the results directory
            with open(result_file, 'w') as f:
                json.dump(data, f)

            print(f"Successfully mirrored results to {result_file}")

        except Exception as e:
            print(f"CRITICAL ERROR during processing: {e}")
            sys.exit(1)

    print("Task completed flawlessly. Exiting.")
    sys.exit(0) # Exit Code 0 will trigger a COMPLETED event in your Go scheduler

if __name__ == "__main__":
    main()