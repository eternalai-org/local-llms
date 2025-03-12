import requests
import os
from loguru import logger
import time
import shutil
import subprocess
import logging
from tqdm import tqdm
from pathlib import Path
from typing import Dict, Tuple, Optional
import httpx

# Constants
BASE_URL = "https://gateway.lighthouse.storage/ipfs/"
DEFAULT_OUTPUT_DIR = Path.cwd() / "llms-storage"
SLEEP_TIME = 60
MAX_ATTEMPTS = 10
CHUNK_SIZE = 1024
POSTFIX_MODEL_PATH = ".gguf"
HTTPX_TIMEOUT = 100

def check_downloaded_model(filecoin_hash: str) -> bool:
    """
    Check if the model is already downloaded and optionally save metadata.
    
    Args:
        filecoin_hash: IPFS hash of the model metadata
        output_file: Optional path to save metadata JSON
    
    Returns:
        bool: Whether the model is already downloaded
    """    
    try:
        local_path = DEFAULT_OUTPUT_DIR / f"{filecoin_hash}{POSTFIX_MODEL_PATH}"
        
        # Check if model exists
        is_downloaded = local_path.exists()
            
        if is_downloaded:
            logger.info(f"Model already exists at: {local_path}")
            
        return is_downloaded
        
    except requests.RequestException as e:
        logger.error(f"Failed to fetch model metadata: {e}")
        return False


def download_file(file_info: Dict[str, str], model_dir: Path, client: httpx.Client, chunk_size: int = CHUNK_SIZE) -> Tuple[bool, str]:
    """Download a file using a shared HTTPX client with optimized resume support."""
    file_name = file_info["file"]
    hash_value = file_info["hash"]
    file_url = f"{BASE_URL}{hash_value}"
    file_path = model_dir / file_name

    file_size = file_path.stat().st_size if file_path.exists() else 0

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            headers = {"Range": f"bytes={file_size}-"} if file_size > 0 else {}
            mode = "ab" if file_size > 0 else "wb"

            with client.stream("GET", file_url, headers=headers) as response:

                total_size = int(response.headers.get("content-length", 0))
                if file_size == total_size and file_size > 0:
                    return True, file_name

                if file_size > 0:
                    logger.warning(f"Partial file appears corrupted, restarting download: {file_path}")
                    os.remove(file_path)
                    file_size = 0

                with open(file_path, mode) as f:
                    progress_bar = tqdm(
                        total=total_size,
                        unit="iB",
                        unit_scale=True,
                        desc=file_name,
                        ncols=100,
                        initial=file_size,
                    )

                    for chunk in response.iter_bytes(chunk_size):
                        if chunk:
                            f.write(chunk)
                            progress_bar.update(len(chunk))

                progress_bar.close()

            return True, file_name

        except (httpx.RequestError, httpx.TimeoutException) as e:
            logger.error(f"Download failed: {e} (Attempt {attempt}/{MAX_ATTEMPTS})")

        if attempt < MAX_ATTEMPTS:
            time.sleep(SLEEP_TIME)

    return False, file_name


def download_and_extract_model(filecoin_hash: str, chunk_size: int = CHUNK_SIZE, output_dir: Optional[Path] = None) -> Optional[Path]:
    """Sequentially download and extract model files."""
    output_dir = output_dir or DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    local_path = output_dir / f"{filecoin_hash}{POSTFIX_MODEL_PATH}"

    if local_path.exists():
        logger.info(f"Model already exists at: {local_path}")
        return local_path

    input_link = f"{BASE_URL}{filecoin_hash}"
    logger.info(f"Fetching model metadata from: {input_link}")
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with httpx.Client(follow_redirects=True, timeout=HTTPX_TIMEOUT) as client:
                response = client.get(input_link)
                data = response.json()

                model_name = data['model']
                temp_dir = Path(model_name)
                temp_dir.mkdir(exist_ok=True)

                num_files = data['num_of_file']
                logger.info(f"Downloading {model_name}: {num_files} files")

                successful_downloads = 0
                print(f"[LAUNCHER_LOGGER] [MODEL_INSTALL] --step {successful_downloads}-{num_files} --hash {filecoin_hash}")
                for idx, file_info in enumerate(data['files']):
                    success, file_name = download_file(file_info, temp_dir, client, chunk_size)
                    if success:
                        successful_downloads += 1
                        print(f"[LAUNCHER_LOGGER] [MODEL_INSTALL] --step {successful_downloads}-{num_files} --hash {filecoin_hash}")
                    else:
                        logger.error(f"Download failed --step {idx + 1}-{num_files} --hash {filecoin_hash}")
                        

                if successful_downloads != num_files:
                    logger.error(f"Failed to download all files: {successful_downloads}/{num_files}")    
                else:
                    break                
                
        except httpx.RequestError as e:
                logger.error(f"Failed to fetch model metadata: {e} (Attempt {attempt}/{MAX_ATTEMPTS})")
                time.sleep(SLEEP_TIME)
    try:
        logger.info("Extracting model files...")
        extract_cmd = (
            f"cat '{temp_dir}/{model_name}'.zip.part-* | "
            f"pigz -p {os.cpu_count()} -d | "
            f"tar -xf - -C '{output_dir}'"
        )
        subprocess.run(extract_cmd, shell=True, check=True, capture_output=True, text=True)

        source_path = output_dir / model_name / model_name
        shutil.move(source_path, local_path)

        shutil.rmtree(output_dir / model_name, ignore_errors=True)

        logger.info(f"Model successfully downloaded to {local_path}")
        return local_path

    except Exception as e:
        logger.error(f"Error: {str(e)}")
    finally:
        if temp_dir.exists() and local_path.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)

    return None
