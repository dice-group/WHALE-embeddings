#!/bin/bash

# Define the base directory
BASE_DIR="/scratch/hpc-prf-whale/WHALE-output/embeddings"

# List of subfolders to archive
SUBFOLDERS=("adr" "geo" "hcalendar" "hcard" "hlisting" "hrecipe" "hresume" "hreview" "rdfa" "species" "xfn")

# Change to the base directory
cd "$BASE_DIR" || { echo "Base directory not found: $BASE_DIR"; exit 1; }

# Loop over each subfolder and create an archive
for SUBFOLDER in "${SUBFOLDERS[@]}"; do
    if [ -d "$SUBFOLDER" ]; then
        # Create the archive file
        ARCHIVE_NAME="${SUBFOLDER}.tar.gz"
        tar -czvf "$ARCHIVE_NAME" "$SUBFOLDER"
        echo "Created archive: $ARCHIVE_NAME"
    else
        echo "Subfolder not found: $SUBFOLDER"
    fi
done
