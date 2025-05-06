#!/bin/bash

log_file="schedule_jobs_MICRODATA.log"

# Clear previous log file content
> "$log_file"

dataset_folder="/scratch/hpc-prf-whale/WHALE-data/domain_specific/domain_dataset/microdata_dataset"

max_array_size=1000
files_per_array=10  # Number of files per job array
submitted_jobs=0

# Parse options
batch_path=""
custom_start_index=0

while getopts ":b:s:" opt; do
  case $opt in
    b)
      batch_path="$OPTARG"
      echo "Batch path set to $batch_path" | tee -a "$log_file"
      ;;
    s)
      if [[ $OPTARG =~ ^[0-9]+$ ]]; then
        custom_start_index="$OPTARG"
        echo "Custom start index set to $custom_start_index" | tee -a "$log_file"
      else
        echo "Invalid custom start index provided. It must be a non-negative integer." | tee -a "$log_file"
        exit 1
      fi
      ;;
    \?)
      echo "Invalid option: -$OPTARG" | tee -a "$log_file"
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." | tee -a "$log_file"
      exit 1
      ;;
  esac
done

# Shift away the parsed options
shift $((OPTIND -1))

###############################################################################
# Memory and CPU Calculation Based on Largest File in a Batch
#   Below 1 MB        => 1 CPU, no explicit memory
#   1 MB to 100 MB    => 4 CPUs, 2GB
#   100 MB to 300 MB  => 16 CPUs, 6GB
#   300 MB to 1 GB    => 64 CPUs, 8GB
#   Above 1 GB        => 128 CPUs, 15GB
###############################################################################
function determine_resources() {
    local largest_size_bytes=$1

    local cpus=1
    local memory_req="2G"  # Empty => no explicit --mem

    if   [ "$largest_size_bytes" -lt $((1*1024*1024)) ]; then
        # < 1 MB
        cpus=1
        memory_req="2G"   # no explicit memory
    elif [ "$largest_size_bytes" -lt $((100*1024*1024)) ]; then
        # 1 MB to 100 MB
        cpus=4
        memory_req="3G"
    elif [ "$largest_size_bytes" -lt $((300*1024*1024)) ]; then
        # 100 MB to 300 MB
        cpus=16
        memory_req="6G"
    elif [ "$largest_size_bytes" -lt $((1024*1024*1024)) ]; then
        # 300 MB to 1 GB
        cpus=64
        memory_req="128G"
    else
        # > 1 GB
        cpus=128
        memory_req="220G"
    fi

    # Output result: <cpus> <memory_req>
    echo "$cpus $memory_req"
}

# Function to submit a job array
submit_job_array() {
    local batch_file="$1"
    local job_count=$(wc -l < "$batch_file")

    # Calculate the number of jobs to be submitted (each job processes 'files_per_array' files)
    array_count=$(( (job_count + files_per_array - 1) / files_per_array ))

    # Extract batch identifier from batch file name
    batch_name=$(basename "$batch_file")
    batch_id="${batch_name%.*}"  # Remove extension

    # Determine the largest file in this batch
    local largest_file_size=0
    while IFS= read -r file_path; do
        if [ -f "$file_path" ]; then
            size=$(stat -c%s "$file_path")
            if [ "$size" -gt "$largest_file_size" ]; then
                largest_file_size=$size
            fi
        else
            echo "File not found (skipping in resource calc): $file_path" | tee -a "$log_file"
        fi
    done < "$batch_file"

    # Get the recommended CPU and memory for this batch
    read cpus mem_required <<< "$(determine_resources "$largest_file_size")"

    echo "Largest file size in batch $batch_file: $largest_file_size bytes" | tee -a "$log_file"
    echo "=> CPU cores: $cpus, Memory: ${mem_required:-'DEFAULT/UNSPECIFIED'}" | tee -a "$log_file"

    # Build SBATCH options
    sbatch_options="-J MICRODATA_${batch_id} --array=0-$(($array_count - 1))%1000 --export=batch_file="$(realpath "$batch_file")" --cpus-per-task=$cpus"

    if [ -n "$mem_required" ]; then
        sbatch_options="$sbatch_options --mem=$mem_required"
    fi

    sbatch $sbatch_options run_embeddings_array.sh
    submitted_jobs=$((submitted_jobs + job_count))

    echo "Submitted job array for batch file $batch_file as MICRODATA_${batch_id} ($job_count files in $array_count jobs) with CPU=$cpus, Mem=$mem_required" | tee -a "$log_file"
}

wait_for_completion() {
    while true; do
        pending_jobs=$(squeue -u $USER -t PENDING,RUNNING -h -r | wc -l)
        available_slots=$((50000 - pending_jobs))
        if [ "$available_slots" -ge "$max_array_size" ]; then
            break
        fi
        echo "Waiting for jobs to complete. Current pending/running jobs: $pending_jobs" | tee -a "$log_file"
        sleep 60
    done
}

# Decide if we have a batch path or not
if [ -n "$batch_path" ]; then
    if [ -f "$batch_path" ]; then
        echo "Batch file provided: $batch_path" | tee -a "$log_file"
        # Sort the batch file by file sizes
        temp_sorted=$(mktemp)
        while IFS= read -r line; do
            if [ -f "$line" ]; then
                size=$(stat -c%s "$line")
                echo "$size $line"
            else
                echo "0 $line"
            fi
        done < "$batch_path" | sort -n | awk '{ $1=""; sub(/^ /, ""); print }' > "$temp_sorted"
        mv "$temp_sorted" "$batch_path"
        batch_files=("$batch_path")
    elif [ -d "$batch_path" ]; then
        echo "Batch directory provided: $batch_path" | tee -a "$log_file"
        batch_files=( $(ls "$batch_path"/*.txt | sort -V) )
    else
        echo "Provided batch path is neither a file nor a directory: $batch_path" | tee -a "$log_file"
        exit 1
    fi
else
    # Use default batch_dir
    batch_dir="/scratch/hpc-prf-whale/WHALE-embeddings/batch_files_microdata"
    mkdir -p "$batch_dir"
    batch_files=( $(ls "$batch_dir"/*.txt 2>/dev/null | sort -V) )
fi

# If no batch files found, create them
if [ ${#batch_files[@]} -eq 0 ]; then
    if [ -n "$batch_path" ]; then
        echo "No batch files found in provided batch path: $batch_path" | tee -a "$log_file"
        exit 1
    else
        echo "No batch files found in $batch_dir. Creating batch files..." | tee -a "$log_file"

        # Ensure the dataset folder exists
        if [ ! -d "$dataset_folder" ]; then
            echo "Dataset folder not found: $dataset_folder" | tee -a "$log_file"
            exit 1
        fi

        echo "Reading all data paths from $dataset_folder into an array (with sizes)..." | tee -a "$log_file"
        IFS=$'\n' read -d '' -r -a sorted_entries < <(find "$dataset_folder" -type f -printf "%s %p\n" | sort -n)
        declare -a data_paths

        # echo "Build an array of (size, path), then sort ascending by size" | tee -a "$log_file"
        # mapfile -t all_files < <(find "$dataset_folder" -type f)
        # declare -A file_sizes_map
        # for f in "${all_files[@]}"; do
        #     if [ -f "$f" ]; then
        #         size=$(stat -c%s "$f" 2>/dev/null)
        #         file_sizes_map["$f"]=$size
        #     fi
        # done

        # echo "Sort all files by size ascending" | tee -a "$log_file"
        # IFS=$'\n'
        # sorted_by_size=( $(for file in "${all_files[@]}"; do
        #     echo "${file_sizes_map[$file]} $file"
        # done | sort -n -k1,1) )
        # IFS=' '

        # echo "Rebuild the data_paths array with the sorted paths" | tee -a "$log_file"
        # data_paths=()
        # for entry in "${sorted_by_size[@]}"; do
        #     # entry is "size path"
        #     # separate them:
        #     size_part="${entry%% *}"
        #     path_part="${entry#* }"
        #     data_paths+=("$path_part")
        # done

        echo "Build the data_paths array by extracting the file path"
        for entry in "${sorted_entries[@]}"; do
            # Remove the size part (everything up to the first space)
            path_part="${entry#* }"
            data_paths+=("$path_part")
        done

        total_files=${#data_paths[@]}
        echo "Total number of files found: $total_files" | tee -a "$log_file"
        if [ "$custom_start_index" -ge "$total_files" ]; then
            echo "Custom start index ($custom_start_index) is >= total number of files ($total_files)." | tee -a "$log_file"
            exit 1
        fi

        batch_index=$custom_start_index
        while [ $batch_index -lt $total_files ]; do
            end_index=$((batch_index + max_array_size * files_per_array - 1))
            if [ $end_index -ge $total_files ]; then
                end_index=$((total_files - 1))
            fi

            batch_file="$batch_dir/batch_${batch_index}_${end_index}.txt"

            if [ -f "$batch_file" ]; then
                echo "Batch file $batch_file already exists. Skipping creation." | tee -a "$log_file"
            else
                echo "Creating batch file $batch_file for indices $batch_index to $end_index" | tee -a "$log_file"
                for ((i = $batch_index; i <= $end_index; i++)); do
                    echo "${data_paths[$i]}" >> "$batch_file"
                done
            fi
            batch_index=$((end_index + 1))
        done

        # Refresh the list of batch files
        batch_files=( $(ls "$batch_dir"/*.txt | sort -V) )
    fi
else
    echo "Batch files found. Proceeding with job submission." | tee -a "$log_file"
fi

# Iterate over batch files and submit jobs
for batch_file in "${batch_files[@]}"; do
    if [ -f "$batch_file" ]; then
        # Extract start_index and end_index from filename
        batch_name=$(basename "$batch_file")
        batch_name_no_ext="${batch_name%.*}"  # Remove extension

        # Extract the indices using parameter expansion and pattern matching
        if [[ $batch_name_no_ext =~ ^batch_([0-9]+)_([0-9]+)$ ]]; then
            start_index="${BASH_REMATCH[1]}"
            end_index="${BASH_REMATCH[2]}"
            output_dir="/scratch/hpc-prf-whale/WHALE-output/embeddings/microdata/logs/MICRODATA_batch_${start_index}_${end_index}"
        else
            # If batch file name does not match expected pattern
            output_dir="/scratch/hpc-prf-whale/WHALE-output/embeddings/microdata/logs/MICRODATA_${batch_name_no_ext}"
        fi

        # Check if the batch starts before the custom start index
        if [[ "$start_index" != "" && "$start_index" -lt "$custom_start_index" ]]; then
            echo "Skipping batch file $batch_file (starts at index $start_index, before custom start index $custom_start_index)" | tee -a "$log_file"
            continue
        fi

        # Check if the output directory already exists
        if [ -d "$output_dir" ]; then
            echo "Output directory $output_dir already exists. Skipping batch $batch_file." | tee -a "$log_file"
            continue
        fi

        wait_for_completion
        submit_job_array "$batch_file"
    else
        echo "Batch file not found: $batch_file" | tee -a "$log_file"
    fi
done

echo "Total number of jobs submitted: $submitted_jobs" | tee -a "$log_file"
echo "All jobs have been scheduled." | tee -a "$log_file"
