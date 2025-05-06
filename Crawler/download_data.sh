#!/bin/bash

# Base directory to store downloaded files (relative to repo root)
base_dir="data/raw"
mkdir -p "$base_dir"

# Download the list of file URLs
wget -q -O file.list http://webdatacommons.org/structureddata/2023-12/files/file.list

# Count total number of URLs
total_urls=$(grep -c '^http' file.list)
current=0

# Process each URL in the list
while IFS= read -r url; do
    # Skip empty lines
    [[ -z "$url" ]] && continue

    ((current++))

    # Extract the filename from the URL
    filename=$(basename "$url")

    # Extract metadata name (between 'html-' and '.nq')
    if [[ "$filename" =~ html-(.*)\.nq ]]; then
        metadata="${BASH_REMATCH[1]}"
    elif [[ "$filename" =~ [^.]+\.(.*)\.nq ]]; then
        metadata="${BASH_REMATCH[1]}"
    else
        echo "❌ Could not extract metadata from $filename"
        continue
    fi

    # Create folder if not exists
    target_dir="$base_dir/$metadata"
    mkdir -p "$target_dir"

    # Show progress
    echo "[$current / $total_urls] Downloading $filename into $target_dir"

    # Download with progress bar
    wget --show-progress -q -P "$target_dir" "$url"
done < file.list

echo "✅ All downloads completed and stored in '$base_dir/'"
