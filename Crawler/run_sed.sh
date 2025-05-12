#!/bin/sh
shopt -s nullglob

# ----------------------------------------------------------------------------
# Sed-based cleaning of all domain_dataset subfolders
# ----------------------------------------------------------------------------
BASE_DIR="data/domain_dataset"

# sed expression: replace blank-prefixed _:XYZ with <http://whale.data.dice-research.org/resource#XYZ>
SED_EXPR='s/\([[:space:]]\|^\)_:\([a-zA-Z0-9]*\)/\1<http:\/\/whale.data.dice-research.org\/resource#\2>/g'

# Function to draw a progress bar
# Usage: draw_progress <current> <total>
draw_progress() {
    local current=$1 total=$2
    local percent=$(( current * 100 / total ))
    local filled=$(( percent / 2 ))
    local empty=$(( 50 - filled ))
    printf "\rProgress: ["
    printf "%0.s#" $(seq 1 $filled)
    printf "%0.s." $(seq 1 $empty)
    printf "] %d%% (%d/%d)" "$percent" "$current" "$total"
}

# Loop over each metadata folder
for search_dir in "$BASE_DIR"/*/; do
    [ -d "$search_dir" ] || continue
    echo -e "\n🔧 Processing directory: $search_dir"
    
    # gather files
    mapfile -d '' files < <(find "$search_dir" -type f -print0)
    total=${#files[@]}
    if [ "$total" -eq 0 ]; then
        echo "No files found in $search_dir, skipping."
        continue
    fi
    counter=0
    
    # process files
    for file in "${files[@]}"; do
        sed -i -e "$SED_EXPR" "$file"
        ((counter++))
        draw_progress "$counter" "$total"
    done

    echo -e "\n✅ Completed cleaning $total files in '$search_dir'."
done
