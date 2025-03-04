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
import json

# Constants
BASE_URL = "https://gateway.lighthouse.storage/ipfs/"
DEFAULT_OUTPUT_DIR = Path.cwd() / "models"
SLEEP_TIME = 5
MAX_ATTEMPTS = 3
CHUNK_SIZE = 16384
POSTFIX_MODEL_PATH = ".gguf"

def setup_logging() -> logging.Logger:
    """Configure and return a logger instance with detailed settings"""
    logging.basicConfig(
        level=logging.DEBUG,  # Changed to DEBUG for more detail
        format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

def check_downloaded_model(filecoin_hash: str, output_file: str = None) -> bool:
    """Check if the model files are already downloaded."""
    if output_file is None:
        output_file = Path.cwd() / f"{filecoin_hash}.json"
    input_link = os.path.join(BASE_URL, filecoin_hash)
    logger = setup_logging()
    response = requests.get(input_link, timeout=10)
    response.raise_for_status()
    logger.debug(f"Metadata response status: {response.status_code}")
    data = response.json()
    logger.debug(f"Metadata JSON parsed successfully: {len(data)} keys") 
    model_name = data['model']
    local_path = os.path.join(str(DEFAULT_OUTPUT_DIR), model_name + POSTFIX_MODEL_PATH)
    metadata = {
        "is_downloaded": False,
        "model_path": local_path,
    }
    if os.path.exists(local_path):
        logger.info(f"Model already exists at: {local_path}")
        metadata["is_downloaded"] = True
        with open(output_file, "w") as f:
            json.dump(metadata, f)
        logger.info(f"Metadata saved to: {output_file}")
        return True
    return False


def download_file(file_info: Dict[str, str], model_dir: Path, chunk_size: int = CHUNK_SIZE) -> Tuple[bool, str]:
    """Download a file with resume support using HTTPX."""
    logger = logging.getLogger(__name__)
    file_name = file_info["file"]
    hash_value = file_info["hash"]
    file_url = f"{BASE_URL}{hash_value}"
    file_path = model_dir / file_name

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            headers = {}
            existing_size = file_path.stat().st_size if file_path.exists() else 0

            with httpx.Client(follow_redirects=True, timeout=60) as client:
                # Check total file size
                response = client.head(file_url)
                response.raise_for_status()
                total_size = int(response.headers.get("content-length", 0))

                # Resume download if possible
                if existing_size and existing_size < total_size:
                    headers["Range"] = f"bytes={existing_size}-"
                    logger.info(f"Resuming download from {existing_size} bytes")

                # Start downloading
                with client.stream("GET", file_url, headers=headers) as response:
                    response.raise_for_status()

                    progress_bar = tqdm(
                        total=total_size,
                        initial=existing_size,
                        unit="iB",
                        unit_scale=True,
                        desc=file_name,
                        leave=True,
                        ncols=100,
                    )

                    with open(file_path, "ab" if existing_size else "wb") as f:
                        for chunk in response.iter_bytes(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                                progress_bar.update(len(chunk))

                    progress_bar.close()

            logger.info(f"Download completed: {file_name} - Size: {os.path.getsize(file_path)} bytes")
            return True, file_name

        except (httpx.RequestError, httpx.TimeoutException) as e:
            logger.error(f"Download failed: {e} (Attempt {attempt}/{MAX_ATTEMPTS})")

        if attempt < MAX_ATTEMPTS:
            logger.info(f"Retrying in {SLEEP_TIME} seconds...")
            time.sleep(SLEEP_TIME)
        else:
            return False, file_name

    return False, file_name

def download_and_extract_model(filecoin_hash: str, max_workers: Optional[int] = None, chunk_size: int = 1024, output_dir: Path = DEFAULT_OUTPUT_DIR) -> None:
    """
    Download and extract model files from IPFS link in parallel with detailed logging.
    
    Args:
        input_link: IPFS gateway URL containing model metadata
        max_workers: Maximum number of parallel downloads
    """
    input_link = os.path.join(BASE_URL, filecoin_hash)
    logger = setup_logging()
    model_dir = None
    
    try:
        logger.info(f"Initiating download process for: {input_link}")
        
        # Fetch metadata
        logger.debug(f"Sending GET request to fetch metadata from {input_link}")
        response = requests.get(input_link, timeout=10)
        response.raise_for_status()
        logger.debug(f"Metadata response status: {response.status_code}")
        data = response.json()
        logger.debug(f"Metadata JSON parsed successfully: {len(data)} keys") 

        # Setup paths
        model_name = data['model']
        local_path = str(output_dir/model_name)  + POSTFIX_MODEL_PATH
        if os.path.exists(local_path):
            logger.info(f"Model already exists at: {local_path}")
            return local_path
        num_files = data['num_of_file']
        model_dir = Path(model_name)
        logger.info(f"Model identified: {model_name} with {num_files} files")
        
        logger.debug(f"Creating directory if not exists: {model_dir}")
        model_dir.mkdir(exist_ok=True)
        logger.info(f"Working directory prepared: {model_dir.absolute()}")

        # Optimize max_workers
        max_workers = max_workers or min(os.cpu_count() * 2, num_files)
        logger.info(f"Configuring parallel download with {max_workers} workers "
                   f"(CPU count: {os.cpu_count()})")

        # Parallel downloads
        successful_downloads = 0
        logger.debug(f"Starting ThreadPoolExecutor with {max_workers} workers")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(download_file, file_info, model_dir, chunk_size): file_info
                for file_info in data['files']
            }
            logger.debug(f"Submitted {len(future_to_file)} download tasks")
            
            for future in as_completed(future_to_file):
                success, file_name = future.result()
                if success:
                    successful_downloads += 1
                    logger.info(f"Download success #{successful_downloads}: {file_name}")
                else:
                    logger.warning(f"Download failed: {file_name}")
        
        logger.info(f"Download phase completed: {successful_downloads}/{num_files} successful")
        if successful_downloads != num_files:
            raise RuntimeError(f"Partial download failure: {successful_downloads}/{num_files} files")

        # Extraction
        logger.debug(f"Preparing extraction directory: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        extract_cmd = (
            f"cat {model_dir}/{model_name}.zip.part-* | "
            f"pigz -p {os.cpu_count()} -d | "
            f"tar -xf - -C {output_dir}"
        )
        logger.info(f"Executing extraction command: {extract_cmd}")
        process = subprocess.run(extract_cmd, shell=True, check=True, capture_output=True, text=True)
        logger.debug(f"Extraction output: {process.stdout}")
        logger.info("Extraction completed successfully")

        # Cleanup
        cleanup_cmd = f"rm -rf {model_dir}"
        logger.info(f"Executing cleanup command: {cleanup_cmd}")
        process = subprocess.run(cleanup_cmd, shell=True, check=True, capture_output=True, text=True)
        logger.info(f"Cleanup completed - temporary files removed")

        logger.info(f"Process completed successfully for {model_name}: "
                   f"{num_files} files processed")
        cur_model_path = output_dir/model_name/model_name
        logger.info(f"Model path: {cur_model_path}")
        logger.info(f"Moving model to expected path: {local_path}")
        shutil.move(cur_model_path, local_path)
        logger.info(f"Model moved successfully to: {local_path}")

    except requests.RequestException as e:
        logger.error(f"Network error during metadata fetch: {str(e)}", exc_info=True)
        raise
    except subprocess.CalledProcessError as e:
        logger.error(f"Extraction process failed: {str(e)}", exc_info=True)
        logger.debug(f"Process stderr: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}", exc_info=True)
        raise
    finally:
        if model_dir and model_dir.exists():
            try:
                logger.debug(f"Performing cleanup in finally block for {model_dir}")
                shutil.rmtree(model_dir, ignore_errors=True)
                shutil.rmtree(output_dir/model_name, ignore_errors=True)
                logger.info("Cleanup in finally block completed")
            except Exception as e:
                logger.error(f"Cleanup in finally block failed: {str(e)}", exc_info=True)
    return local_path

def parse_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description="Download and extract model files from IPFS")
    parser.add_argument(
        "--filecoin-hash",
        required=True,
        help="IPFS hash of the model metadata (e.g., bafkreieglfaposr5fggc7ebfcok7dupfoiwojjvrck6hbzjajs6nywx6qi)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Maximum number of parallel downloads (defaults to CPU count * 2)"
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1024,
        help="Chunk size for downloading files (default: 1024)"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for model files"
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    download_and_extract_model(args.filecoin_hash, max_workers=args.max_workers, chunk_size = args.chunk_size, output_dir= args.output_dir)