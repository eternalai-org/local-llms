import os
import pickle
import psutil
import subprocess
from loguru import logger
from typing import Optional
from local_llms.download import download_and_extract_model

class LocalLLMManager:
    """Manages a local Large Language Model (LLM) service."""
    
    def __init__(self):
        """Initialize the LocalLLMManager."""
        self.running_model: Optional[str] = None
        self.process: Optional[subprocess.Popen] = None
        self.port: Optional[int] = None

    def start(self, hash: str = None, port: int = 8080, host: str = "0.0.0.0") -> bool:
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
        if self.running_model is not None:
            logger.warning(f"Service already running with model: {self.running_model}")
            return False
            
        if not hash:
            raise ValueError("Filecoin hash is required to start the service")
            
        try:
            logger.info(f"Starting local LLM service for model with hash: {hash}")
            local_model_path = download_and_extract_model(hash)
            
            if not os.path.exists(local_model_path):
                logger.error(f"Model file not found at: {local_model_path}")
                return False
                
            logger.info(f"Local LLM service starting for model: {local_model_path}")
            
            # Run llama-server in the background with additional safety checks
            self.process = subprocess.Popen(
                [
                    "nohup", "llama-server",
                    "--model", local_model_path,
                    "--port", str(port),
                    "--host", host,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                preexec_fn=os.setsid  # Ensures process survives parent termination
            )
            
            self.running_model = os.path.basename(local_model_path)
            self.port = port
            logger.info(f"Local LLM service started successfully on port {port} "
                       f"for model: {self.running_model}")
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
        
    def _dump_running_service(self, name, port):
        """Dump the running service details to a file."""
        service_info = {"name": name, "port": port}
        with open("running_service.pkl", "wb") as f:
            pickle.dump(service_info, f)

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
            name = service_info.get("name")
            pid = service_info.get("pid")

            logger.info(f"Stopping LLM service '{name}' running on port {port} (PID: {pid})...")

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
        