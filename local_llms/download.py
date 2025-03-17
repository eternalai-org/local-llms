import requests
from tqdm import tqdm
import shutil
import time
from pathlib import Path
import httpx
import requests
from pathlib import Path
import time
from local_llms.utils import compute_file_hash, extract_zip
from concurrent.futures import ThreadPoolExecutor, as_completed

# Constants
GATEWAY_URL = "https://gateway.lighthouse.storage/ipfs/"
DEFAULT_OUTPUT_DIR = Path.cwd() / "llms-storage"
SLEEP_TIME = 60
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

def download_single_file(file_info: dict, folder_path: Path, max_attempts: int = MAX_ATTEMPTS) -> bool:
    """
    Download a single file from Lighthouse and verify its SHA256 hash, with retries.

    Args:
        file_info (dict): Contains 'cid', 'file_hash', and 'file_name'.
        folder_path (Path): Directory to save the file.
        max_attempts (int): Number of retries on failure.

    Returns:
        tuple: (Path to file if successful, None) or (None, error message).
    """
    cid = file_info["cid"]
    expected_hash = file_info["file_hash"]
    file_name = file_info["file_name"]
    file_path = folder_path / file_name
    attempts = 0

    if file_path.exists():
        computed_hash = compute_file_hash(file_path)
        if computed_hash == expected_hash:
            print(f"File {cid} already exists with correct hash.")
            return file_path, None
        else:
            print(f"File {cid} already exists but hash mismatch. Retrying...")
            file_path.unlink()

    while attempts < max_attempts:
        try:
            url = GATEWAY_URL + cid
            response = requests.get(url, stream=True, timeout=100)

            if response.status_code == 200:
                total_size = int(response.headers.get("content-length", 0))
                with file_path.open("wb") as f, tqdm(
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    desc=f"Downloading {file_name}",
                    ncols=80
                ) as progress:
                    for chunk in response.iter_content(chunk_size=4096):
                        if chunk:
                            f.write(chunk)
                            progress.update(len(chunk))

                computed_hash = compute_file_hash(file_path)
                if computed_hash == expected_hash:
                    print(f"File {cid} downloaded and verified successfully.")
                    return file_path, None
                else:
                    print(f"Hash mismatch for {cid}. Expected {expected_hash}, got {computed_hash}. Retrying...")
                    file_path.unlink()
            else:
                print(f"Failed to download {cid}. Status code: {response.status_code}")

        except Exception as e:
            print(f"Exception while downloading {cid}: {e}")

        attempts += 1
        if attempts < max_attempts:
            print(f"Retrying in {SLEEP_TIME} seconds... (Attempt {attempts + 1}/{max_attempts})")
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
    max_workers = min(2, num_of_files)  # Limit concurrency to 4 or number of files
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
    return result_paths

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
    
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            print(f"Downloading model metadata (attempt {attempt}/{MAX_ATTEMPTS})")
            
            with httpx.Client(follow_redirects=True, timeout=HTTPX_TIMEOUT) as client:
                response = client.get(input_link)
                response.raise_for_status()
                data = response.json()
                data["filecoin_hash"] = filecoin_hash
                folder_name = data["folder_name"]
                
                paths = download_files_from_lighthouse(data)
                if not paths:
                    print("Failed to download model files")
                    continue
                    
        except Exception as e:
            print(f"Download attempt {attempt} failed: {e}")
            if attempt < MAX_ATTEMPTS:
                backoff = min(SLEEP_TIME * (2 ** (attempt - 1)), 300)  # Exponential backoff capped at 5 min
                print(f"Retrying in {backoff} seconds")
                time.sleep(backoff)

    try:
        print("Extracting downloaded files")
        folder_path = Path.cwd()/folder_name
        extract_zip(paths)      
        source_path = folder_path / folder_name
        print(f"Moving model to {local_path}")
        if source_path.exists():
            shutil.move(source_path, local_path)
        else:
            print(f"Model folder not found: {source_path}")
        if folder_path.exists():
            shutil.rmtree(folder_path, ignore_errors=True)
        print(f"Model download complete: {local_path}")
        return local_path
    except Exception as e:
        print(f"Failed to extract files: {e}")
    
    print("All download attempts failed")
    return None
            


            