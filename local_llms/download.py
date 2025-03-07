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
SLEEP_TIME = 5
MAX_ATTEMPTS = 3
CHUNK_SIZE = 8192
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
    """Download a file with optimized resume support using HTTPX."""
    logger = logging.getLogger(__name__)
    file_name = file_info["file"]
    filecoin_hash_value = file_info["hash"]
    file_url = f"{BASE_URL}{filecoin_hash_value}"
    file_path = model_dir / file_name
    
    # Create a separate directory for resume info files
    resume_info_dir = model_dir / ".resume_info"
    resume_info_dir.mkdir(exist_ok=True)
    resume_info_path = resume_info_dir / f"{file_name}.resume"

    # Early validation of existing file
    total_size = 0
    existing_size = 0
    if file_path.exists():
        existing_size = file_path.stat().st_size
        # Quick check before making HEAD request
        if existing_size > 0:
            with httpx.Client(follow_redirects=True, timeout=30.0) as client:
                response = client.head(file_url)
                response.raise_for_status()
                total_size = int(response.headers.get("content-length", 0))
                if existing_size == total_size:
                    logger.info(f"File already fully downloaded: {file_name}")
                    resume_info_path.unlink(missing_ok=True)
                    return True, file_name

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            headers = {}
            resume_position = existing_size

            # Only store minimal resume info when needed
            if resume_position > 0:
                with open(resume_info_path, "w") as rf:
                    rf.write(f"{file_url}\n{resume_position}")

            with httpx.Client(follow_redirects=True, timeout=120.0) as client:
                # Get total size if not already known
                if total_size == 0:
                    response = client.head(file_url)
                    response.raise_for_status()
                    total_size = int(response.headers.get("content-length", 0))

                if resume_position > 0 and resume_position < total_size:
                    headers["Range"] = f"bytes={resume_position}-"
                    logger.info(f"Resuming download from {resume_position}/{total_size} bytes "
                               f"({resume_position/total_size*100:.1f}%)")

                with client.stream("GET", file_url, headers=headers) as response:
                    response.raise_for_status()
                    remaining_size = int(response.headers.get("content-length", 0))
                    
                    # Handle inconsistent size (fallback)
                    if remaining_size == 0:
                        logger.warning(f"Server did not provide content-length for {file_name}, assuming full content.")
                        remaining_size = total_size - resume_position  # Fallback to assumed size

                    current_total = resume_position + remaining_size

                    if current_total > total_size > 0:
                        logger.warning(f"Server reported inconsistent size for {file_name}. Adjusting download size.")
                        current_total = total_size

                    with tqdm(total=total_size, initial=resume_position, unit="iB", 
                            unit_scale=True, desc=file_name, leave=True, ncols=100) as progress_bar:
                        # Use binary append mode only if resuming
                        mode = "ab" if resume_position > 0 else "wb"
                        with open(file_path, mode) as f:
                            bytes_since_flush = 0
                            save_threshold = max(10 * 1024 * 1024, chunk_size * 10)  # Adaptive threshold
                            
                            for chunk in response.iter_bytes(chunk_size=chunk_size):
                                if not chunk:
                                    continue
                                    
                                chunk_size_to_write = min(len(chunk), total_size - resume_position)
                                if chunk_size_to_write <= 0:
                                    break
                                    
                                f.write(chunk[:chunk_size_to_write])
                                resume_position += chunk_size_to_write
                                progress_bar.update(chunk_size_to_write)
                                bytes_since_flush += chunk_size_to_write

                                if bytes_since_flush >= save_threshold:
                                    # Ensure the file is open before flushing
                                    if not f.closed:
                                        f.flush()
                                        os.fsync(f.fileno())
                                    with open(resume_info_path, "w") as rf:
                                        rf.write(f"{file_url}\n{resume_position}")
                                    bytes_since_flush = 0

                        # Final flush and sync
                        if not f.closed:
                            f.flush()
                            os.fsync(f.fileno())

                    # Verify completion
                    final_size = file_path.stat().st_size
                    if final_size != total_size:
                        logger.warning(f"Download size mismatch: got {final_size}, expected {total_size}")
                        if final_size < total_size:
                            logger.info("Retrying download from the last valid position.")
                        else:
                            logger.error("Download corrupted or incomplete.")
                        return False, file_name

            logger.info(f"Download completed: {file_name} - Size: {final_size} bytes")
            resume_info_path.unlink(missing_ok=True)
            return True, file_name

        except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout) as e:
            logger.error(f"Connection error on attempt {attempt}/{MAX_ATTEMPTS}: {e}")
            if attempt < MAX_ATTEMPTS:
                wait_time = SLEEP_TIME * (2 ** (attempt - 1))
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
        except Exception as e:
            logger.error(f"Download attempt {attempt} failed: {e}")
            if attempt < MAX_ATTEMPTS:
                time.sleep(SLEEP_TIME)

    return False, file_name

def download_and_extract_model(filecoin_hash: str, max_workers: Optional[int] = None, 
                              chunk_size: int = 1024, output_dir: Optional[Path] = None) -> Optional[Path]:
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
    
    # Track downloads state for resuming
    resume_state_file = output_dir / f"{filecoin_hash}.resume_state"
    
    if local_path.exists():
        logger.info(f"Model already exists at: {local_path}")
        resume_state_file.unlink(missing_ok=True)  # Clean up resume state if exists
        return local_path
    
    interrupted = False
    connection_lost = False
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
        
        # Save model metadata for resume capability
        with open(resume_state_file, "w") as f:
            f.write(f"{filecoin_hash}\n{model_name}\n{data['num_of_file']}")
        
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
        resume_state_file.unlink(missing_ok=True)  # Clean up resume state
        
        logger.info(f"Model successfully downloaded to {local_path}")
        return local_path

    except KeyboardInterrupt:
        interrupted = True
        logger.warning("Download interrupted by user. Partial files are preserved for resuming.")
        logger.info(f"Run the same command again to resume download.")
        return None
    except requests.RequestException as e:
        connection_lost = True
        logger.error(f"Network error: {e}. Partial files are preserved for resuming.")
        logger.info(f"Run the same command again to resume download.")
    except (httpx.RequestError, httpx.TimeoutException) as e:
        connection_lost = True
        logger.error(f"Connection lost: {e}. Partial files are preserved for resuming.")
        logger.info(f"Run the same command again to resume download.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Extraction failed: {e.stderr}")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        # Only clean up if not interrupted by user and connection wasn't lost
        if temp_dir and temp_dir.exists() and not (interrupted or connection_lost):
            shutil.rmtree(temp_dir, ignore_errors=True)
            logger.debug(f"Temporary directory {temp_dir} removed")
        elif (interrupted or connection_lost) and temp_dir and temp_dir.exists():
            logger.info(f"Temporary directory {temp_dir} preserved for resumable download")
    
    return None