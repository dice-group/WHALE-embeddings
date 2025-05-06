#!/bin/sh

#SBATCH -J "SED COMMAND"
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -p normal
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH -t 02-00:00:00
#SBATCH -A hpc-prf-dsg
#SBATCH -o %x-%j.log
#SBATCH -e %x-%j.log
#SBATCH --mail-type=FAIL,TIME_LIMIT_80,BEGIN
#SBATCH --mail-user=sshivam@mail.uni-paderborn.de

cd /scratch/hpc-prf-whale/WHALE-data/domain_specific/domain_dataset/
search_dir="jsonld_dataset"

sed_command='s/\([[:space:]]\|^\)_:\([a-zA-Z0-9]*\)/\1<http:\/\/whale.data.dice-research.org\/resource#\2>/g'

echo "Get all files"
mapfile -d '' files < <(find "$search_dir" -type f -print0)

total=${#files[@]}
counter=0

echo "🔧 Processing $total files in '$search_dir'..."
echo

# Function to draw progress bar
draw_progress() {
    local progress=$((counter * 100 / total))
    local filled=$((progress / 2))
    local empty=$((50 - filled))
    printf "\rProgress: ["
    printf "%0.s#" $(seq 1 $filled)
    printf "%0.s." $(seq 1 $empty)
    printf "] %d%% (%d/%d)" "$progress" "$counter" "$total"
}

# Loop with progress
for file in "${files[@]}"; do
    sed -i -e "$sed_command" "$file"
    ((counter++))
    draw_progress
done

echo -e "\n✅ Done processing all files."

exit 0
~