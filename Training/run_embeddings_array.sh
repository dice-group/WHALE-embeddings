#!/bin/sh

#SBATCH -N 1
#SBATCH -n 1
#SBATCH -t 07-00:00:00
#SBATCH -A hpc-prf-whale
#SBATCH --partition=gpu
#SBATCH --gres=gpu:a100:2
#SBATCH --mail-type=FAIL,TIME_LIMIT,ARRAY_TASKS
#SBATCH --mail-user=sshivam@mail.uni-paderborn.de
#SBATCH -o /scratch/hpc-prf-whale/WHALE-output/embeddings/microdata/logs/%x/%A_%a.log
#SBATCH -e /scratch/hpc-prf-whale/WHALE-output/embeddings/microdata/logs/%x/%A_%a.log

JOB_ID=$SLURM_ARRAY_JOB_ID

module load lang/Miniforge3/24.1.2-0
source $(conda info --base)/etc/profile.d/conda.sh
conda activate dice

# Read the data paths from the batch file
start_line=$((SLURM_ARRAY_TASK_ID * 10 + 1))
end_line=$((start_line + 9))

# Use a temporary file to store the sed output
temp_file=$(mktemp)

sed -n "${start_line},${end_line}p" "$batch_file" > "$temp_file"

# Initialize the log file for failures
failed_log_file="/scratch/hpc-prf-whale/WHALE-output/embeddings/microdata/failed_failed_failed_failed_FAILED.log"

# Process each file
while IFS= read -r data_path; do
    file_name=$(basename "$data_path")
    format_from_dataset_folder=$(basename "$(dirname "$data_path")" | sed 's/_dataset//')

    echo "$data_path"
    
    # Run the dicee command and check for assertion errors
    dicee --path_single_kg $data_path \
            --path_to_store_single_run /dev/shm/Keci_GPU_$file_name\
            --model Keci \
            --num_epochs 500 \
            --p 0 \
            --q 1 \
            --embedding_dim 256 \
            --scoring_technique NegSample \
            --eval_model None \
            --batch_size 100_000 \
            --optim Adopt \
            --disable_checkpointing
    
    # Check if the dicee command failed with an assertion error
    if [ $? -ne 0 ]; then
        echo "Assertion error occurred for $data_path. Logging to $failed_log_file"
        echo "$data_path" >> "$failed_log_file"
    fi
done < "$temp_file"

# Clean up the temporary file
rm -f "$temp_file"


echo "Compress the outputs ..."
tar -czvf /dev/shm/${JOB_ID}_${SLURM_ARRAY_TASK_ID}.tar.gz -C /dev/shm $(ls /dev/shm/)

echo "Move the tar file to the desired directory ..."
destination_dir="/scratch/hpc-prf-whale/WHALE-output/embeddings/microdata/models"
# Ensure the destination directory exists
mkdir -p "$destination_dir"

mv /dev/shm/${JOB_ID}_${SLURM_ARRAY_TASK_ID}.tar.gz "$destination_dir"

echo "DONE"
conda deactivate
exit 0
~