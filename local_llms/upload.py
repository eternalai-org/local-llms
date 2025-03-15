import os
import json
import shutil
import time
import hashlib
import tempfile
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from lighthouseweb3 import Lighthouse
from dotenv import load_dotenv

load_dotenv()

def compute_file_hash(file_path: Path, hash_algo: str = "sha256") -> str:
    """Compute the hash of a file."""
    hash_func = getattr(hashlib, hash_algo)()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()

def upload_to_lighthouse(file_path: Path):
    """
    Upload a file to Lighthouse.storage and measure the time taken.
    Note: Assumes lighthouse_web3.upload is synchronous; adjust if async.
    """
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # Size in MB
        file_hash = compute_file_hash(file_path)

        start_time = time.time()
        file_name = os.path.basename(file_path)
        lh = Lighthouse(token=os.getenv("LIGHTHOUSE_API_KEY"))
        response = lh.upload(str(file_path))  # Convert Path to string
        elapsed_time = time.time() - start_time
        upload_speed = file_size / elapsed_time if elapsed_time > 0 else 0

        print(f"Uploaded {file_path}: {elapsed_time:.2f}s, {upload_speed:.2f} MB/s")
        if "data" in response and "Hash" in response["data"]:
            cid = response["data"]["Hash"]
            return {"cid": cid, "file_hash": file_hash, "size_mb": file_size, "file_name": file_name}, None
        else:
            return None, "No CID in response"
    except Exception as e:
        print(f"Upload failed for {file_path}: {str(e)}")
        return None, str(e)

def compress_folder(model_folder: str, zip_chunk_size: int = 128, threads: int = 1) -> str:
    """
    Compress a folder into split parts using tar, pigz, and split.
    """
    if not os.path.isdir(model_folder):
        raise ValueError(f"Invalid folder path: {model_folder}")
    if not all(shutil.which(cmd) for cmd in ["tar", "pigz", "split"]):
        raise RuntimeError("Required commands (tar, pigz, split) not found.")

    temp_dir = tempfile.mkdtemp()
    output_prefix = os.path.join(temp_dir, os.path.basename(model_folder) + ".zip.part-")
    tar_command = (
        f"tar -cf - '{model_folder}' | pigz --best -p {threads} | "
        f"split -b {zip_chunk_size}M - '{output_prefix}'"
    )

    try:
        subprocess.run(tar_command, shell=True, check=True)
        print(f"Compressed to {temp_dir}")
        return temp_dir
    except subprocess.CalledProcessError as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise RuntimeError(f"Compression failed: {e}")

def upload_folder_to_lighthouse(
    folder_name: str, zip_chunk_size=512, max_retries=20, threads=16, max_workers=4, **kwargs
):
    """
    Upload a folder to Lighthouse.storage by compressing it into parts and uploading in parallel.
    """
    folder_path = Path(folder_name)
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    
    metadata = {
        "folder_name": folder_name,
        "chunk_size_mb": zip_chunk_size,
        "files": [],
        **kwargs,
    }
    metadata_path = Path.cwd() / f"{folder_name}.json"
    temp_dir = None

    try:
        # Compress the folder
        temp_dir = compress_folder(folder_path, zip_chunk_size, threads)
        part_files = [
            os.path.join(temp_dir, f) for f in sorted(os.listdir(temp_dir))
            if f.startswith(f"{folder_name}.zip.part-")
        ]
        metadata["num_of_files"] = len(part_files)
        print(f"Uploading {len(part_files)} parts to Lighthouse.storage...")

        # Parallel upload with retries
        errors = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            def upload_with_retry(part_path):
                for attempt in range(max_retries):
                    file_info, error = upload_to_lighthouse(part_path)
                    if file_info:
                        return file_info, None
                    print(f"Retry {attempt + 1}/{max_retries} for {part_path}")
                    time.sleep(2)
                return None, f"Failed after {max_retries} attempts"

            future_to_part = {
                executor.submit(upload_with_retry, part): part for part in part_files
            }
            for future in as_completed(future_to_part):
                part_path = future_to_part[future]
                file_info, error = future.result()
                if file_info:
                    metadata["files"].append(file_info)
                else:
                    errors.append((part_path, error))

        # Save metadata and handle results
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=4)

        if errors:
            error_msg = "\n".join(f"{path}: {err}" for path, err in errors)
            print(f"Completed with {len(errors)} errors:\n{error_msg}")
            return None, f"Partial upload failure: {len(errors)} parts failed"
        print("All parts uploaded successfully!")
        
        # upload metadata to Lighthouse
        metadata_info, error = upload_to_lighthouse(metadata_path)
        if metadata_info:
            print(f"Metadata uploaded: {metadata_info['cid']}")
            return metadata, None
        else:
            return None, error
        
    except Exception as e:
        print(f"Upload process failed: {str(e)}")
        return None, str(e)
    finally:
        if temp_dir and os.path.exists(temp_dir) and not errors:
            shutil.rmtree(temp_dir, ignore_errors=True)
            print(f"Cleaned up temporary directory: {temp_dir}")