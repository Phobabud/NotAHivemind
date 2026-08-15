# Use an official Python runtime as a parent images
FROM python:3.11

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Run script with entry point and default arguments so any new arguments will be appended
# 1. ENTRYPOINT defines the fixed executable and script
ENTRYPOINT ["python", "args_test.py"]

# 2. CMD provides default arguments (optional)
CMD ["default arg...uh oh you forgot something!"]