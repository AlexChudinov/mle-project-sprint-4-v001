#!/bin/bash

set -e

# Default values
CONTEXT=.
OUTPUT_DIR=$CONTEXT/ym
VERBOSE=false
FORCE=false

function download_data() {
    wget https://storage.yandexcloud.net/mle-data/ym/tracks.parquet -O $OUTPUT_DIR/tracks.parquet

    wget https://storage.yandexcloud.net/mle-data/ym/catalog_names.parquet -O $OUTPUT_DIR/catalog_names.parquet

    wget https://storage.yandexcloud.net/mle-data/ym/interactions.parquet -O $OUTPUT_DIR/interactions.parquet
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--force)
            FORCE=true
            shift
            ;;
        -c|--context)
            CONTEXT="$2"
            shift 2
            ;;
        -o|--output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  -f, --force                   Force download"
            echo "  -c, --context CONTEXT         Root directory (default: .)"
            echo "  -o, --output-dir OUTPUT_DIR   Output directory (default: $OUTPUT_DIR)"
            echo "  -v, --verbose                 Enable verbose mode"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

if [ -f "$OUTPUT_DIR/tracks.parquet" ] && [ -f "$OUTPUT_DIR/catalog_names.parquet" ] && [ -f "$OUTPUT_DIR/interactions.parquet" ] && ! $FORCE; then
echo All files are already downloaded, if you need to forcely download them, please, use the -f or --force option
exit 0
fi

# Use the parameters
if [ "$VERBOSE" = true ]; then
    echo "Name: $NAME"
    echo "Count: $COUNT"
    echo "Output directory: $OUTPUT_DIR"
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Download data
download_data
