import os
import shutil
import hashlib
from typing import List
import subprocess
import shutil
import tempfile
import subprocess
from pathlib import Path


def compress_folder(model_folder: str, zip_chunk_size: int = 128, threads: int = 1) -> str:
    """
    Compress a folder into split parts using tar, pigz, and split.
    """
    temp_dir = tempfile.mkdtemp()
    output_prefix = os.path.join(temp_dir, os.path.basename(model_folder) + ".zip.part-")
    tar_command = (
        f"{os.environ['TAR_COMMAND']} -cf - '{model_folder}' | "
        f"{os.environ['PIGZ_COMMAND']} --best -p {threads} | "
        f"split -b {zip_chunk_size}M - '{output_prefix}'"
    )
    try:
        subprocess.run(tar_command, shell=True, check=True)
        print(f"{tar_command} completed successfully")
        return temp_dir
    except subprocess.CalledProcessError as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise RuntimeError(f"Compression failed: {e}")

def extract_zip(paths: List[Path]):
    # Use the absolute path only once.
    target_abs = Path.cwd().absolute()
    target_dir = f"'{target_abs}'"
    print(f"Extracting files to: {target_dir}")

    # Get absolute paths for required commands.
    cat_path = os.environ.get("CAT_COMMAND")
    pigz_cmd = os.environ.get("PIGZ_COMMAND")
    tar_cmd = os.environ.get("TAR_COMMAND")
    if not (cat_path and pigz_cmd and tar_cmd):
        raise RuntimeError("Required commands (cat, TAR_COMMAND, PIGZ_COMMAND) not found.")

    # Sort paths by their string representation.
    sorted_paths = sorted(paths, key=lambda p: str(p))
    # Quote each path after converting to its absolute path.
    paths_str = " ".join(f"'{p.absolute()}'" for p in sorted_paths)
    print(f"Extracting files: {paths_str}")

    cpus = os.cpu_count() or 1
    extract_command = (
        f"{cat_path} {paths_str} | "
        f"{pigz_cmd} -p {cpus} -d | "
        f"{tar_cmd} -xf - -C {target_dir}"
    )
    subprocess.run(extract_command, shell=True, check=True, capture_output=True, text=True)
    print(f"{extract_command} completed successfully")

def compute_file_hash(file_path: Path, hash_algo: str = "sha256") -> str:
    """Compute the hash of a file."""
    hash_func = getattr(hashlib, hash_algo)()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()

