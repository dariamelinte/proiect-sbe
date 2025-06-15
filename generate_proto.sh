#!/bin/bash

# Create protos directory if it doesn't exist
mkdir -p protos

# Generate Python code from proto file
python -m grpc_tools.protoc \
    --proto_path=./protos \
    --python_out=./protos \
    --grpc_python_out=./protos \
    ./protos/message.proto 