import os
import pickle
import psutil
import subprocess
from pathlib import Path
from loguru import logger
from typing import Optional
from local_llms.download import download_and_extract_model

class LocalLLMManager:
    """Manages a local Large Language Model (LLM) service."""
    
    def __init__(self):
        """Initialize the LocalLLMManager."""       
        self.pickle_file = Path.cwd()/ "running_service.pkl"

    def start(self, hash: str, port: int = 8080, host: str = "0.0.0.0", context_length: int = 4096) -> bool:
        """
        Start the local LLM service in the background.
        
        Args:
            hash (str, optional): Filecoin hash of the model to download and run
            port (int): Port number for the LLM service (default: 8080)
            
        Returns:
            bool: True if service started successfully, False otherwise
            
        Raises:
            ValueError: If hash is not provided when no model is running
        """
        if not hash:
            raise ValueError("Filecoin hash is required to start the service")
         
        try:
            logger.info(f"Starting local LLM service for model with hash: {hash}")
            local_model_path = download_and_extract_model(hash)
            if os.path.exists("running_service.pkl"):
                with open("running_service.pkl", "rb") as f:
                    service_info = pickle.load(f)
                    if service_info.get("hash") == hash:
                        logger.warning(f"Model '{hash}' is already running on port {service_info.get('port')}")
                        return True
                    else:
                        logger.info(f"Stopping existing model '{hash}' running on port {service_info.get('port')}")
                        self.stop()
            
            if not os.path.exists(local_model_path):
                logger.error(f"Model file not found at: {local_model_path}")
                return False
                
            logger.info(f"Local LLM service starting for model: {local_model_path}")
            
            # Run llama-server in the background with additional safety checks
            process = subprocess.Popen(
                [
                    "nohup", "llama-server",
                    "--jinja",
                    "--model", local_model_path,
                    "--port", str(port),
                    "--host", host,
                    "-c", str(context_length)
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                preexec_fn=os.setsid  # Ensures process survives parent termination
            )
            self._dump_running_service(hash, port, process.pid)
            logger.info(f"Local LLM service started successfully on port {port} "
                       f"for model: {hash}")
            return True
            
        except FileNotFoundError:
            logger.error("llama-server executable not found in system PATH")
            return False
        except subprocess.SubprocessError as e:
            logger.error(f"Failed to start local LLM service: {str(e)}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error starting LLM service: {str(e)}", exc_info=True)
            return False
        
    def _dump_running_service(self, hash, port, pid):
        """Dump the running service details to a file."""
        service_info = {"hash": hash, "port": port, "pid": pid}
        with open("running_service.pkl", "wb") as f:
            pickle.dump(service_info, f)

    def get_running_model(self) -> Optional[str]:
        """
        Get the name of the currently running model.
        
        Returns:
            Optional[str]: Name of the currently running model, or None if no model is running
        """
        if not os.path.exists("running_service.pkl"):
            return None
        
        with open("running_service.pkl", "rb") as f:
            service_info = pickle.load(f)
            return service_info.get("hash")

    def stop(self) -> bool:
        """
        Stop the running LLM service.

        Returns:
            bool: True if the service stopped successfully, False otherwise.
        """
        if not os.path.exists("running_service.pkl"):
            logger.warning("No running LLM service to stop.")
            return False

        try:
            # Load service details from the pickle file
            with open("running_service.pkl", "rb") as f:
                service_info = pickle.load(f)
            
            port = service_info.get("port")
            hash = service_info.get("hash")
            pid = service_info.get("pid")

            logger.info(f"Stopping LLM service '{hash}' running on port {port} (PID: {pid})...")

            # Terminate process by PID
            if psutil.pid_exists(pid):
                process = psutil.Process(pid)
                process.terminate()
                process.wait(timeout=5)  # Allow process to shut down gracefully
                
                if process.is_running():  # Force kill if still alive
                    logger.warning("Process did not terminate, forcing kill.")
                    process.kill()

            # Remove the tracking file
            os.remove("running_service.pkl")
            logger.info("LLM service stopped successfully.")
            return True

        except Exception as e:
            logger.error(f"Error stopping LLM service: {str(e)}", exc_info=True)
            return False
        