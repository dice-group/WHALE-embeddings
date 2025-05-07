#!/bin/bash
shopt -s nullglob

# ----------------------------------------------------------------------------
# ENVIRONMENT DETECTION
# ----------------------------------------------------------------------------
if command -v sbatch &>/dev/null; then
    is_cluster=1
    echo "🌐 Detected cluster environment."
else
    is_cluster=0
    echo "🏠 Running in local environment."
fi

# ----------------------------------------------------------------------------
# PARSE OPTIONS
# ----------------------------------------------------------------------------
batch_path=""
custom_start_index=0
while getopts ":b:s:" opt; do
  case $opt in
    b) batch_path="$OPTARG" ;;  
    s)
      if [[ $OPTARG =~ ^[0-9]+$ ]]; then
        custom_start_index="$OPTARG"
      else
        echo "❌ Invalid start index: must be non-negative integer."
        exit 1
      fi
      ;;
    \?) echo "❌ Invalid option: -$OPTARG"; exit 1 ;;  
    :)  echo "❌ Option -$OPTARG requires an argument."; exit 1 ;;
  esac
done
shift $((OPTIND-1))

# ----------------------------------------------------------------------------
# RESOURCE DETERMINATION FUNCTION
# ----------------------------------------------------------------------------
determine_resources() {
    local largest_size_bytes=$1
    # Local: minimal resources
    if [ "$is_cluster" -eq 0 ]; then
        echo "1 2G"
        return
    fi
    # Cluster: scale by size
    local cpus=1 memory_req="2G"
    if   [ "$largest_size_bytes" -lt $((1*1024*1024)) ]; then
        cpus=1;   memory_req="2G"
    elif [ "$largest_size_bytes" -lt $((100*1024*1024)) ]; then
        cpus=4;   memory_req="3G"
    elif [ "$largest_size_bytes" -lt $((300*1024*1024)) ]; then
        cpus=16;  memory_req="6G"
    elif [ "$largest_size_bytes" -lt $((1024*1024*1024)) ]; then
        cpus=64;  memory_req="128G"
    else
        cpus=128; memory_req="220G"
    fi
    echo "$cpus $memory_req"
}

# ----------------------------------------------------------------------------
# WAIT FOR SBATCH SLOTS (cluster only)
# ----------------------------------------------------------------------------
wait_for_completion() {
    if [ "$is_cluster" -eq 1 ]; then
        local max_array_size=1000
        while true; do
            pending=$(squeue -u $USER -t PENDING,RUNNING -h -r | wc -l)
            slots=$((50000 - pending))
            [ "$slots" -ge "$max_array_size" ] && break
            echo "Waiting: $pending jobs running/pending..."
            sleep 60
        done
    fi
}

# ----------------------------------------------------------------------------
# SUBMIT OR RUN ONE BATCH FILE
# ----------------------------------------------------------------------------
submit_job_array() {
    local batch_file="$1"
    local batch_name=$(basename "$batch_file" .txt)
    if [ "$is_cluster" -eq 1 ]; then
        # Cluster
        local files_per_array=10
        local count=$(wc -l < "$batch_file")
        local array_count=$(( (count + files_per_array - 1) / files_per_array ))
        local largest=0 size
        while read -r fp; do
            [ -f "$fp" ] && size=$(stat -c%s "$fp") || size=0
            (( size > largest )) && largest=$size
        done < "$batch_file"
        read cpus mem <<< "$(determine_resources $largest)"
        echo "Largest: $largest bytes → CPUs=$cpus, MEM=$mem" | tee -a "$log_file"
        sbatch_opts="-J ${metafolder}_${batch_name} --array=0-$((array_count-1))%1000"
        sbatch_opts+=" --export=batch_file=$(realpath "$batch_file")"
        sbatch_opts+=" --cpus-per-task=$cpus"
        [ -n "$mem" ] && sbatch_opts+=" --mem=$mem"
        # sbatch $sbatch_opts run_embeddings_array.sh
        ((submitted_jobs+=count))
        echo "Submitted ${metafolder}_${batch_name} ($count files, $array_count jobs)" | tee -a "$log_file"
    else
        # Local
        echo "➡️ Running locally for $batch_file" | tee -a "$log_file"
        while read -r fp; do
            # ./run_embeddings_array.sh "$fp"
            ((submitted_jobs++))
        done < "$batch_file"
    fi
}

# ----------------------------------------------------------------------------
# MAIN: iterate datasets
# ----------------------------------------------------------------------------
for dataset_folder in data/domain_dataset/*/; do
    [ -d "$dataset_folder" ] || continue
    dataset=$(basename "$dataset_folder")
    metafolder=${dataset^^}
    log_file="schedule_jobs_${metafolder}.log"
    :> "$log_file"
    submitted_jobs=0
    echo -e "\n=== Dataset=$dataset, META=$metafolder ===" | tee -a "$log_file"

    # Determine batch files location
    if [ -n "$batch_path" ]; then
        if [ -f "$batch_path" ]; then
            batch_files=("$batch_path")
        elif [ -d "$batch_path" ]; then
            batch_files=("$batch_path"/*.txt)
        else
            echo "❌ Invalid batch path: $batch_path" | tee -a "$log_file"; exit 1
        fi
    else
        batch_dir="data/batch_files_${dataset}"
        mkdir -p "$batch_dir"
        batch_files=("$batch_dir"/*.txt)
    fi

    # Filter out literal pattern if no matches
    if [ ${#batch_files[@]} -eq 1 ] && [[ "${batch_files[0]}" == *"*.txt" ]]; then
        batch_files=()
    fi

    # If none, create sorted batches
    if [ ${#batch_files[@]} -eq 0 ]; then
        echo "Building batches for $dataset..." | tee -a "$log_file"
        readarray -t entries < <(find "$dataset_folder" -type f -printf "%s %p\n" | sort -n)
        mapfile -t paths < <(printf "%s\n" "${entries[@]}" | cut -d' ' -f2-)
        total=${#paths[@]}
        [ $custom_start_index -ge $total ] && { echo "❌ start_index >= total" | tee -a "$log_file"; exit 1; }
        idx=$custom_start_index
        while [ $idx -lt $total ]; do
            end=$((idx + 1000*10 -1))
            [ $end -ge $total ] && end=$((total-1))
            file="data/batch_files_${dataset}/batch_${idx}_${end}.txt"
            if [ ! -f "$file" ]; then
                echo "Creating $file" | tee -a "$log_file"
                for ((i=idx;i<=end;i++)); do echo "${paths[i]}" >> "$file"; done
            fi
            idx=$((end+1))
        done
        batch_files=("data/batch_files_${dataset}"/*.txt)
    else
        echo "Found existing batch files for $dataset." | tee -a "$log_file"
    fi

    # Submit/run
    for bf in "${batch_files[@]}"; do
        [ -f "$bf" ] || continue
        start=${bf##*batch_}; start=${start%%_*}
        [ -n "$custom_start_index" ] && (( start < custom_start_index )) && continue
        outdir="embeddings/$dataset/logs/${metafolder}_batch_${start}_*"
        [ -d "$outdir" ] && { echo "Skipping $bf" | tee -a "$log_file"; continue; }
        wait_for_completion
        submit_job_array "$bf"
    done

    echo "Total jobs for $dataset: $submitted_jobs" | tee -a "$log_file"
done

echo "All datasets scheduled."
