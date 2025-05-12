#!/bin/sh

#SBATCH -N 1
#SBATCH -n 1
#SBATCH -t 07-00:00:00
#SBATCH -A hpc-prf-whale
#SBATCH --partition=gpu
#SBATCH --gres=gpu:a100:2

# these are exported by the scheduler via --export=
dataset=${dataset:?Need dataset}
metafolder=${metafolder:?Need metafolder}

JOB_ID=$SLURM_ARRAY_JOB_ID
TASK_ID=$SLURM_ARRAY_TASK_ID

module load lang/Miniforge3/24.1.2-0
source $(conda info --base)/etc/profile.d/conda.sh
conda activate dice

# Read the data paths from the batch file
start_line=$((TASK_ID * 10 + 1))
end_line=$((start_line + 9))

# Use a temporary file to store the sed output
temp_file=$(mktemp)
sed -n "${start_line},${end_line}p" "$batch_file" > "$temp_file"

# Initialize the log file for failures
failed_log_file="embeddings/${dataset}/logs/${metafolder}_${JOB_ID}_${TASK_ID}_failed.log"
mkdir -p "$(dirname "$failed_log_file")"

# Process each file
while IFS= read -r data_path; do
    file_name=$(basename "$data_path")
    format_from_dataset_folder=$(basename "$(dirname "$data_path")" | sed 's/_dataset//')

    echo "▶️  Processing $data_path"
    
    # Run the dicee command and check for assertion errors
    dicee --path_single_kg "$data_path" \
          --path_to_store_single_run "/dev/shm/Keci_GPU_$file_name" \
          --model Keci \
          --num_epochs 500 \
          --p 0 \
          --q 1 \
          --embedding_dim 256 \
          --scoring_technique NegSample \
          --eval_model None \
          --batch_size 100_000 \
          --optim Adopt \

    if [ $? -ne 0 ]; then
        echo "❌ Assertion error for $data_path"
        echo "$data_path" >> "$failed_log_file"
    fi
done < "$temp_file"

rm -f "$temp_file"

echo "▶️  Compressing embeddings outputs..."
tar -czvf "/dev/shm/${JOB_ID}_${TASK_ID}.tar.gz" -C /dev/shm $(ls /dev/shm/)

destination_dir="embeddings/${dataset}/models/${metafolder}_${JOB_ID}_${TASK_ID}"
mkdir -p "$destination_dir"

mv "/dev/shm/${JOB_ID}_${TASK_ID}.tar.gz" "$destination_dir/"

echo "✅ Done. Archived results in $destination_dir"
conda deactivate
exit 0
~