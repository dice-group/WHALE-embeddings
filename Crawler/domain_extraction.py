#!/usr/bin/env python3

import os
import glob
import logging
import gzip
import argparse
import csv
import shutil
from urllib.parse import urlparse
from joblib import Parallel, delayed
from tqdm import tqdm

# ----------------------------------------------------------------------------
# DOMAIN PROCESSOR (no external domain list; group by base URL)
# ----------------------------------------------------------------------------
class DomainProcessor:
    def __init__(self, metadata, output_dir):
        self.metadata = metadata
        self.output_dir = output_dir
        self.no_domain_lines = []

        # Prepare CSV log file under data/domain_logs
        log_dir = os.path.join("data", "domain_logs")
        os.makedirs(log_dir, exist_ok=True)
        self.log_file = os.path.join(log_dir, f"{metadata}.csv")

    def get_base_url(self, url):
        url = url.strip("<>")
        netloc = urlparse(url).netloc
        return netloc[4:] if netloc.startswith("www.") else netloc

    def process_data(self, file_path):
        pid = os.getpid()
        logging.info(f"[PID {pid}] starting read of {file_path}")
        try:
            file_size = os.path.getsize(file_path)
            logging.info(f"[PID {pid}] file size: {file_size} bytes")
        except Exception:
            logging.warning(f"Could not get size for {file_path}")
        # now begin reading data
        local_out = {}
        line_count = 0

        # Attempt to read and bucket lines by base URL
        try:
            with gzip.open(file_path, "rt") as fh:
                for line in fh:
                    line_count += 1
                    parts = line.split()
                    if len(parts) < 2:
                        continue
                    url_part = parts[-2]
                    base = self.get_base_url(url_part)
                    local_out.setdefault(base, []).append(line)
        except Exception:
            logging.exception(f"Error reading '{file_path}'")
            return False  # indicate no data processed due to error

        logging.info(
            f"[PID {pid}] finished read of {file_path}: "
            f"{line_count} lines processed, {len(local_out)} domains found"
        )
        self.save_results(local_out)
        return True

    def save_results(self, local_out):
        for dom, lines in tqdm(local_out.items(), desc="writing domains", unit="dom"):
            out_file = os.path.join(self.output_dir, f"{dom}.txt")
            os.makedirs(os.path.dirname(out_file), exist_ok=True)

            existing = set()
            if os.path.exists(out_file):
                with open(out_file, "r", encoding="utf-8", errors="ignore") as ex:
                    existing = set(ex.readlines())

            with open(out_file, "a", encoding="utf-8") as out:
                for ln in lines:
                    if ln not in existing:
                        out.write(ln)

        logging.info(f"Written {len(local_out)} domain files to {self.output_dir}")

    def display_counts(self):
        counts = {}
        for fn in glob.glob(os.path.join(self.output_dir, "*.txt")):
            with open(fn, "r", encoding="utf-8", errors="ignore") as f:
                counts[os.path.basename(fn).replace(".txt", "")] = sum(1 for _ in f)

        if not counts:
            logging.warning(f"No triples found for '{self.metadata}'. Skipping CSV log for this metadata.")
            return counts

        # Write counts to CSV with header 'domain, #triples'
        try:
            with open(self.log_file, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["domain", "#triples"])
                for dom, cnt in sorted(counts.items(), key=lambda x: x[1], reverse=True):
                    writer.writerow([dom, cnt])
            logging.info(f"Counts logged to {self.log_file}")
        except Exception:
            logging.exception(f"Failed to write CSV log '{self.log_file}'")
        return counts

# ----------------------------------------------------------------------------
# MAIN DRIVER (no domain_files)
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    parser = argparse.ArgumentParser(
        description="Extract domain-based datasets from structured-data .gz archives."
    )
    parser.add_argument(
        "--num_core",
        type=int,
        default=1,
        help="Number of parallel workers (cores) to use; defaults to 1."
    )
    args = parser.parse_args()
    n_jobs = args.num_core

    RAW_BASE = "data/raw"
    DATASET_BASE = "data/domain_dataset"

    os.makedirs(DATASET_BASE, exist_ok=True)

    for metaf in sorted(os.listdir(RAW_BASE)):
        raw_dir = os.path.join(RAW_BASE, metaf)
        if not os.path.isdir(raw_dir):
            continue

        gz_files = sorted(glob.glob(os.path.join(raw_dir, "*.gz")))
        if not gz_files:
            logging.warning(f"No .gz files in {raw_dir}, skipping metadata '{metaf}'.")
            continue

        proc = DomainProcessor(metaf, os.path.join(DATASET_BASE, metaf))
        logging.info(f"--- Processing METADATA '{metaf}' with {n_jobs} core(s) ---")

        # Process all files
        results = Parallel(n_jobs=n_jobs)(
            delayed(proc.process_data)(fpath) for fpath in gz_files
        )

        # Check if any file produced data
        if len(gz_files) == 1 and not any(results):
            logging.warning(f"Single .gz file in '{metaf}' yielded no data; skipping creation of dataset and CSV.")
            continue

        # Ensure output dir exists for multi-file or successful single-file
        out_dir = os.path.join(DATASET_BASE, metaf)
        os.makedirs(out_dir, exist_ok=True)

        counts = proc.display_counts()
        # If counts empty for single file, remove dir
        if not counts and len(gz_files) == 1:
            logging.warning(f"Removing empty output directory for '{metaf}'.")
            shutil.rmtree(out_dir, ignore_errors=True)

    logging.info("✅ All metadata folders processed.")
