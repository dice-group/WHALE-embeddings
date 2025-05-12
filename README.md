# WHALE-embeddings

This repository contains code related to the resource article _Embedding the Web: An Open Billion-Scale Repository for Knowledge Graph Embeddings_.

## Project Structure

The code of the Embedding the Web approach comprises two main sub-modules:

- **Crawler**: Responsible for downloading and preprocessing the raw RDF data dumps from the WebDataCommons structured data corpus.
- **Training**: Handles job scheduling, batch creation, and execution of embedding training (via Slurm on a cluster or locally with dicee).


```
WHALE-embeddings/
├── Crawler/
│ ├── download_data.sh # Download raw .nq.gz files into data/raw
│ ├── domain_extraction.py # Split triples by domain into data/domain_dataset
│ └── run_sed.sh # Clean domain_dataset files via sed regex
├── Training/
│ ├── schedule_jobs.sh # Scheduler: create batches and submit or run jobs
│ └── run_embeddings_array.sh# Job script: run dicee or sbatch array tasks
├── pipeline.sh # Top-level orchestration: download, extract, schedule
├── file.list_sample # Sample URL list fallback
├── .gitignore
└── README.md # (this file)
```


### Crawler

1. **download_data.sh**  
   - Reads `file.list` (or `file.list_sample` if missing) of HTTP URLs.  
   - Extracts metadata tags (e.g. `html-embedded-jsonld`) and organizes downloads into `data/raw/<metadata>/`.  

2. **domain_extraction.py**  
   - Walks each `data/raw/<metadata>/` folder.  
   - Reads every `.gz`, buckets triples by base-URL domain, and writes per-domain `.txt` files into `data/domain_dataset/<metadata>/`.  
   - Logs triple counts into `data/domain_logs/<metadata>.csv`.

3. **run_sed.sh**

    - Iterates over all subfolders under `data/domain_dataset/`.
    - Applies a `sed` regex to each file, replacing blank-node prefixes (`_:id`) with full resource URIs.
    - Provides an in-console progress bar per folder.

### Training

1. **schedule_jobs.sh**  
   - Loops over all `data/domain_dataset/<dataset>/` folders.  
   - Creates size-sorted batch files under `data/batch_files_<dataset>/`.  
   - On a cluster: submits Slurm array jobs with `sbatch`, exporting `batch_file`, `dataset`, and `metafolder`, plus resource directives (`--cpus-per-task`, `--mem`, `--output`, `--error`).  
   - Locally: iterates each batch file and invokes `dicee` directly for each KG file.

2. **run_embeddings_array.sh**  
   - A Slurm array job script that:  
     - Activates the `dice` environment.  
     - Reads the appropriate slice of `batch_file` (10 lines per task).  
     - Runs `dicee` on each data path, logging failures.  
     - Archives results from `/dev/shm` into `embeddings/<dataset>/models/<metafolder>_<job>_<task>/`.

## Installation

### Prerequisites

- Git  
- Conda (Miniforge or Anaconda)  
- `wget`, `grep`, `gzip`, `sed`, `tar` (standard Unix tools)  
- On-cluster: Slurm (`sbatch`, `squeue`)

### Clone Repository

```bash
git clone https://github.com/dice-group/WHALE-embeddings.git
cd WHALE-embeddings
chmod +x Crawler/*.sh Training/*.sh pipeline.sh
```

## Download URL List
Fetch the full list of structured-data dumps (or use the provided sample):

```
wget -q -O file.list http://webdatacommons.org/structureddata/2023-12/files/file.list
```

If you skip the above, pipeline.sh will fall back to file.list_sample

## Run the Pipeline

```
./pipeline.sh
```

The pipeline will download the data, extract domain-specific datasets, and then schedule (or run locally) the embedding training jobs. Outputs and logs will be stored under `embeddings/`.