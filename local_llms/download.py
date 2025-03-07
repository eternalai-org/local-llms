import argparse
import requests
import os
import time
import shutil
import subprocess
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Tuple, Optional
import httpx
import hashlib

# Constants
BASE_URL = "https://gateway.lighthouse.storage/ipfs/"
DEFAULT_OUTPUT_DIR = Path.cwd() / "llms-storage"
SLEEP_TIME = 10
MAX_ATTEMPTS = 5
CHUNK_SIZE = 16 * 1024
POSTFIX_MODEL_PATH = ".gguf"


def setup_logging() -> logging.Logger:
    """Configure and return a logger instance with detailed settings"""
    logging.basicConfig(
        level=logging.DEBUG,  # Changed to DEBUG for more detail
        format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

def check_downloaded_model(filecoin_hash: str) -> bool:
    """
    Check if the model is already downloaded and optionally save metadata.
    
    Args:
        filecoin_hash: IPFS hash of the model metadata
        output_file: Optional path to save metadata JSON
    
    Returns:
        bool: Whether the model is already downloaded
    """
    logger = logging.getLogger(__name__)
    
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


def download_file(file_info: Dict[str, str], model_dir: Path, chunk_size: int = CHUNK_SIZE) -> Tuple[bool, str]:
    """Download a file using HTTPX with retry mechanism."""
    logger = logging.getLogger(__name__)
    file_name = file_info["file"]
    hash_value = file_info["hash"]
    file_url = f"{BASE_URL}{hash_value}"
    file_path = model_dir / file_name

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            # Start downloading the file
            with httpx.Client(follow_redirects=True, timeout=60) as client:
                response = client.head(file_url)
                response.raise_for_status()
                total_size = int(response.headers.get("content-length", 0))

                with client.stream("GET", file_url) as response:
                    response.raise_for_status()

                    # Progress bar for download
                    with open(file_path, "wb") as f:
                        progress_bar = tqdm(
                            total=total_size,
                            unit="iB",
                            unit_scale=True,
                            desc=file_name,
                            ncols=100,
                        )

                        for chunk in response.iter_bytes(chunk_size):
                            if chunk:
                                f.write(chunk)
                                progress_bar.update(len(chunk))

                    progress_bar.close()

            logger.info(f"Download completed: {file_name} - Size: {os.path.getsize(file_path)} bytes")
            return True, file_name

        except (httpx.RequestError, httpx.TimeoutException) as e:
            logger.error(f"Download failed: {e} (Attempt {attempt}/{MAX_ATTEMPTS})")
            if file_path.exists():
                os.remove(file_path)

        if attempt < MAX_ATTEMPTS:
            logger.info(f"Retrying in {SLEEP_TIME} seconds...")
            time.sleep(SLEEP_TIME)
        else:
            return False, file_name

    return False, file_name

def download_and_extract_model(filecoin_hash: str, max_workers: Optional[int] = 2, 
                              chunk_size: int = CHUNK_SIZE, output_dir: Optional[Path] = None) -> Optional[Path]:
    """
    Download and extract model files from IPFS link in parallel with detailed logging.
    
    Args:
        filecoin_hash: IPFS hash of the model metadata
        max_workers: Maximum number of parallel downloads
        chunk_size: Size of download chunks in bytes
        output_dir: Directory to save the model files
        
    Returns:
        Path to the downloaded model or None if failed
    """
    output_dir = output_dir or DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(__name__)
    temp_dir = None
    local_path = output_dir / f"{filecoin_hash}{POSTFIX_MODEL_PATH}"
    if local_path.exists():
        logger.info(f"Model already exists at: {local_path}")
        return local_path
        
    try:
        # Fetch metadata
        input_link = f"{BASE_URL}{filecoin_hash}"
        logger.info(f"Initiating download process for: {input_link}")
        
        response = requests.get(input_link, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Setup paths and prepare directories
        model_name = data['model']
        temp_dir = Path(model_name)
        temp_dir.mkdir(exist_ok=True)
        
        num_files = data['num_of_file']
        logger.info(f"Downloading {model_name}: {num_files} files")

        # Configure parallel downloads
        max_workers = max_workers or min(os.cpu_count() * 2, num_files)
        
        # Download files in parallel
        successful_downloads = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(download_file, file_info, temp_dir, chunk_size): file_info
                for file_info in data['files']
            }
            
            for future in as_completed(futures):
                success, file_name = future.result()
                if success:
                    successful_downloads += 1
                else:
                    logger.warning(f"Download failed: {file_name}")
        
        if successful_downloads != num_files:
            raise RuntimeError(f"Incomplete download: {successful_downloads}/{num_files} files")

        # Extract files
        extract_cmd = (
            f"cat '{temp_dir}/{model_name}'.zip.part-* | "
            f"pigz -p {os.cpu_count()} -d | "
            f"tar -xf - -C '{output_dir}'"
        )
        logger.info("Extracting model files...")
        subprocess.run(extract_cmd, shell=True, check=True, capture_output=True, text=True)
        
        # Move model file to final location
        source_path = output_dir / model_name / model_name
        logger.info(f"Moving model to {local_path}")
        shutil.move(source_path, local_path)
        
        # Cleanup temp directories
        shutil.rmtree(output_dir / model_name, ignore_errors=True)
        
        logger.info(f"Model successfully downloaded to {local_path}")
        return local_path

    except requests.RequestException as e:
        logger.error(f"Network error: {e}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Extraction failed: {e.stderr}")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        # Ensure cleanup even if errors occurred
        if temp_dir and temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
            logger.debug(f"Temporary directory {temp_dir} removed")
    
    return None