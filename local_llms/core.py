from loguru import logger
import subprocess
from typing import Optional
import os
from local_llms.download import download_and_extract_model

class LocalLLMManager:
    """Manages a local Large Language Model (LLM) service."""
    
    def __init__(self):
        """Initialize the LocalLLMManager."""
        self.running_model: Optional[str] = None
        self.process: Optional[subprocess.Popen] = None
        self.port: Optional[int] = None

    def start(self, hash: str = None, port: int = 8080) -> bool:
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
                    "llama-server",
                    "--model", local_model_path,
                    "--port", str(port),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
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

    def stop(self) -> bool:
        """
        Stop the local LLM service.
        
        Returns:
            bool: True if service stopped successfully, False otherwise
        """
        if not self.process or self.process.poll() is not None:
            logger.warning("No running LLM service to stop")
            return False
            
        try:
            logger.info(f"Stopping local LLM service for model: {self.running_model}")
            self.process.terminate()
            
            # Wait for process to terminate with timeout
            self.process.wait(timeout=10)
            
            logger.info(f"Local LLM service stopped successfully for model: {self.running_model}")
            self._cleanup()
            return True
            
        except subprocess.TimeoutExpired:
            logger.warning("Graceful termination failed, forcing process kill")
            self.process.kill()
            self._cleanup()
            return True
        except Exception as e:
            logger.error(f"Failed to stop local LLM service: {str(e)}", exc_info=True)
            return False

    def status(self) -> dict:
        """
        Check the status of the local LLM service.
        
        Returns:
            dict: Status information including running state, model name, and port
        """
        status_info = {
            "state": "Stopped",
            "model": None,
            "port": None
        }
        
        if self.process and self.process.poll() is None:
            status_info.update({
                "state": "Running",
                "model": self.running_model,
                "port": self.port
            })
            logger.info(f"Local LLM service is running for model: {self.running_model} "
                       f"on port: {self.port}")
        else:
            logger.info("Local LLM service is not running")
            
        return status_info

    def _cleanup(self) -> None:
        """Reset instance variables after stopping the service."""
        self.process = None
        self.running_model = None
        self.port = None

    def get_logs(self) -> Optional[str]:
        """
        Retrieve logs from the running process.
        
        Returns:
            Optional[str]: Process logs if available, None otherwise
        """
        if self.process and self.process.poll() is None:
            try:
                stdout, stderr = self.process.communicate(timeout=1)
                return f"STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
            except subprocess.TimeoutExpired:
                return "Process still running, logs not fully available"
        return None