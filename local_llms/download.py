import requests
import os
import shutil
import time
from pathlib import Path
import httpx
import requests
import hashlib
from pathlib import Path
import time
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

# Constants
GATEWAY_URL = "https://gateway.lighthouse.storage/ipfs/"
DEFAULT_OUTPUT_DIR = Path.cwd() / "llms-storage"
SLEEP_TIME = 30
MAX_ATTEMPTS = 10
CHUNK_SIZE = 1024
POSTFIX_MODEL_PATH = ".gguf"
HTTPX_TIMEOUT = 100

def check_downloaded_model(filecoin_hash: str, output_dir: Path = DEFAULT_OUTPUT_DIR) -> bool:
    """
    Check if the model is already downloaded and optionally save metadata.
    
    Args:
        filecoin_hash: IPFS hash of the model metadata
        output_file: Optional path to save metadata JSON
    
    Returns:
        bool: Whether the model is already downloaded
    """    
    try:
        local_path = output_dir / f"{filecoin_hash}{POSTFIX_MODEL_PATH}"
        
        # Check if model exists
        is_downloaded = local_path.exists()
            
        if is_downloaded:
            print(f"Model already exists at: {local_path}")
            
        return is_downloaded
        
    except requests.RequestException as e:
        print(f"Failed to fetch model metadata: {e}")
        return False

def compute_file_hash(file_path: Path, hash_algo: str = "sha256") -> str:
    """Compute the hash of a file."""
    hash_func = getattr(hashlib, hash_algo)()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()

def download_single_file(file_info: dict, folder_path: Path, max_attempts: int = MAX_ATTEMPTS) -> bool:
    """
    Download a single file from Lighthouse and verify its SHA256 hash, with retries.
    
    Args:
        file_info (dict): Contains 'cid' and 'file_hash'.
        folder_path (Path): Directory to save the file.
        max_attempts (int): Number of retries on failure.
    
    Returns:
        bool: True if successful, False otherwise.
    """
    cid = file_info["cid"]
    expected_hash = file_info["file_hash"]
    file_name = file_info["file_name"]
    file_path = folder_path / file_name
    attempts = 0

    while attempts < max_attempts:
        try:
            # Construct download URL
            url = GATEWAY_URL + cid
            # Stream the download
            response = requests.get(url, stream=True, timeout=100)
            
            if response.status_code == 200:
                # Write file in chunks
                with file_path.open('wb') as f:
                    for chunk in response.iter_content(chunk_size=4096):
                        if chunk:
                            f.write(chunk)
                
                # Verify hash
                computed_hash = compute_file_hash(file_path)
                if computed_hash == expected_hash:
                    print(f"File {cid} downloaded and verified successfully.")
                    return file_path, None
                else:
                    print(f"Hash mismatch for {cid}. Expected {expected_hash}, got {computed_hash}. Retrying...")
                    file_path.unlink()  # Remove incorrect file
            else:
                print(f"Failed to download {cid}. Status code: {response.status_code}")
        
        except Exception as e:
            print(f"Exception while downloading {cid}: {e}")
        
        # Retry logic
        attempts += 1
        if attempts < max_attempts:
            print(f"Retrying in 5 seconds... (Attempt {attempts + 1}/{max_attempts})")
            time.sleep(SLEEP_TIME)
        else:
            print(f"Failed to download {cid} after {max_attempts} attempts.")
            return None, f"Failed to download {cid} after {max_attempts} attempts."

def download_files_from_lighthouse(data: dict) -> bool:
    """
    Download files from Lighthouse concurrently using Filecoin CIDs, verify SHA256 hashes.
    
    Args:
        data (dict): JSON data with 'folder_name' and 'files' list containing 'cid' and 'file_hash'.
    
    Returns:
        bool: True if all files are downloaded and verified successfully, False otherwise.
    """
    result_paths = []
    # Extract folder name and create directory
    folder_name = data["folder_name"]
    folder_path = Path(folder_name)
    folder_path.mkdir(exist_ok=True)
    num_of_files = data["num_of_files"]
    filecoin_hash = data["filecoin_hash"]
    
    # IPFS gateway URL
    max_workers = min(4, num_of_files)  # Limit concurrency to 4 or number of files
    print(f"[LAUNCHER_LOGGER] [MODEL_INSTALL] --step {len(result_paths)}-{num_of_files} --hash {filecoin_hash}")
    # Use ThreadPoolExecutor for concurrent downloads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_file = {
            executor.submit(download_single_file, file_info, folder_path): file_info["cid"]
            for file_info in data["files"]
        }
        for future in as_completed(future_to_file):
            cid = future_to_file[future]
            try:
                path, error = future.result()
                if path:
                    result_paths.append(path)
                    print(f"[LAUNCHER_LOGGER] [MODEL_INSTALL] --step {len(result_paths)}-{num_of_files} --hash {filecoin_hash}")
                else:
                    print(f"Download task for {cid} failed: {error}")
            except Exception as e:
                print(f"Unexpected error for {cid}: {e}")
    
    assert len(result_paths) == num_of_files, f"Failed to download all files: {len(result_paths)} out of {num_of_files}"
    return folder_path

def extract_zip(folder_name: str, output_dir: Path):
    extract_command = (
        f"cat '{folder_name}/{folder_name}'.zip.part-* | "
        f"pigz -p {os.cpu_count()} -d | "
        f"tar -xf - -C '{output_dir}'"
    )
    subprocess.run(extract_command, shell=True, check=True, capture_output=True, text=True)
    print(f"Extracted files to {output_dir}")


def download_model_from_filecoin(filecoin_hash: str, output_dir: Path = DEFAULT_OUTPUT_DIR):
    """
    Download a model from Filecoin using its IPFS hash.
    
    Args:
        filecoin_hash (str): IPFS hash of the model metadata.
        output_dir (Path): Directory to save the downloaded model.
        
    Returns:
        Path or None: Path to the downloaded model if successful, None otherwise.
    """
    output_dir.mkdir(exist_ok=True, parents=True)
    local_path = output_dir / f"{filecoin_hash}{POSTFIX_MODEL_PATH}"
    
    # Check if the model is already downloaded
    if check_downloaded_model(filecoin_hash, output_dir):
        print(f"Using existing model at {local_path}")
        return local_path
    
    # Download the model metadata
    input_link = f"{GATEWAY_URL}{filecoin_hash}"
    folder_path = None
    
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            print(f"Downloading model metadata (attempt {attempt}/{MAX_ATTEMPTS})")
            
            with httpx.Client(follow_redirects=True, timeout=HTTPX_TIMEOUT) as client:
                response = client.get(input_link)
                response.raise_for_status()
                data = response.json()
                data["filecoin_hash"] = filecoin_hash
                folder_name = data["folder_name"]
                
                folder_path = download_files_from_lighthouse(data)
                if not folder_path:
                    print("Failed to download model files")
                    continue
                
                try:
                    print("Extracting downloaded files")
                    extract_zip(folder_name, output_dir)
                    
                    source_path = output_dir / folder_name / folder_name
                    print(f"Moving model to {local_path}")
                    shutil.move(source_path, local_path)
                    shutil.rmtree(output_dir / folder_name)
                    
                    if folder_path.exists():
                        shutil.rmtree(folder_path)
                    
                    print(f"Model download complete: {local_path}")
                    return local_path
                    
                except Exception as e:
                    print(f"Extraction error: {e}")
                    if local_path.exists():
                        local_path.unlink()
                    raise
                    
        except Exception as e:
            print(f"Download attempt {attempt} failed: {e}")
            if attempt < MAX_ATTEMPTS:
                backoff = min(SLEEP_TIME * (2 ** (attempt - 1)), 300)  # Exponential backoff capped at 5 min
                print(f"Retrying in {backoff} seconds")
                time.sleep(backoff)
    
    # Clean up any leftover folder_path if download failed
    if folder_path and folder_path.exists():
        shutil.rmtree(folder_path)
    
    print("All download attempts failed")
    return None
            


            