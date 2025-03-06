#!/bin/bash
set -o pipefail

# Log function to print messages with timestamps
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Error handling function
handle_error() {
  local exit_code=$1
  local error_msg=$2
  log "ERROR: $error_msg (Exit code: $exit_code)"
  
  # Clean up if needed
  if [[ -n "$VIRTUAL_ENV" ]]; then
    log "Deactivating virtual environment..."
    deactivate 2>/dev/null || true
  fi
  
  exit $exit_code
}

command_exists() {
  command -v "$1" &> /dev/null
}

# Step 1: Check and install Homebrew if not present
if ! command_exists brew; then
  export PATH="$HOME/homebrew/bin:$PATH"
fi

# Step 2: Install or Update Python
log "Checking existing Python version..."
python3 --version || log "No Python installation found."

log "Checking system Python..."
if command_exists python3; then
  log "Python is already installed on the system. Skipping Python installation."
else
  log "No Python found. Installing Python using Homebrew..."
  brew install python || handle_error $? "Failed to install Python"
fi

log "Verifying the installed Python version..."
python3 --version || handle_error $? "Python installation verification failed"
log "Python setup complete."

# Step 3: Update PATH in .zshrc
log "Checking if PATH update is needed in .zshrc..."
if ! grep -q 'export PATH="/opt/homebrew/bin:\$PATH"' ~/.zshrc; then
  log "Backing up current .zshrc..."
  cp ~/.zshrc ~/.zshrc.backup.$(date +%Y%m%d%H%M%S) || handle_error $? "Failed to backup .zshrc"
  
  log "Updating PATH in .zshrc..."
  echo 'export PATH="/opt/homebrew/bin:$PATH"' >> ~/.zshrc || handle_error $? "Failed to update .zshrc"
  log "Please restart your terminal or run 'source ~/.zshrc' manually for changes to take effect."
else
  log "PATH already contains Homebrew bin directory."
fi

# Step 4: Install pigz
log "Installing pigz..."
brew install pigz || handle_error $? "Failed to install pigz"
log "pigz installation completed."

# Step 5: Create and activate Python virtual environment
log "Creating virtual environment 'local_llms'..."
python3 -m venv local_llms || handle_error $? "Failed to create virtual environment"

log "Activating virtual environment..."
if [ -f "local_llms/bin/activate" ]; then
  source local_llms/bin/activate || handle_error $? "Failed to activate virtual environment"
else
  handle_error 1 "Virtual environment activation script not found."
fi
log "Virtual environment activated."

# Step 6: Install llama.cpp
log "Checking existing llama.cpp installation..."
if command -v llama-cli &>/dev/null; then
    log "llama.cpp is installed. Checking for updates..."
else
    log "No llama.cpp installation found."
fi

log "Installing/Updating llama.cpp..."
brew install llama.cpp || handle_error $? "Failed to install llama.cpp"
log "llama.cpp installation/update completed."

log "Verifying the installed llama.cpp version..."
hash -r
llama-cli --version || handle_error $? "llama.cpp verification failed"
log "llama.cpp setup complete."

# Step 7: Set up local-llms toolkit
log "Setting up local-llms toolkit..."
pip3 uninstall local-llms -y || log "Warning: local-llms was not previously installed"
pip3 install -q git+https://github.com/eternalai-org/local-llms.git@v1.0.4 || handle_error $? "Failed to install local-llms toolkit"
log "local-llms toolkit setup completed."

log "All steps completed successfully."