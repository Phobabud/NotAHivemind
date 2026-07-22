import sys

def parse_args() -> list:
    if len(sys.argv) < 2:
        raise ValueError("No arguments provided")
    return sys.argv[1:]

print("Arguments received:", parse_args())
