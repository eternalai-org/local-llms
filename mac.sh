#!/bin/bash
set -o pipefail

# Logging functions
log_message() {
    local message="$1"
    if [[ -n "${message// }" ]]; then
        echo "[LAUNCHER_LOGGER] [MODEL_INSTALL_LLAMA] --message \"$message\""
    fi
}

log_error() {
    local message="$1"
    if [[ -n "${message// }" ]]; then
        echo "[LAUNCHER_LOGGER] [MODEL_INSTALL_LLAMA] --error \"$message\"" >&2
    fi
}

# Error handling function
handle_error() {
    local exit_code=$1
    local error_msg=$2
    log_error "$error_msg (Exit code: $exit_code)"
    
    # Clean up if needed
    if [[ -n "$VIRTUAL_ENV" ]]; then
        log_message "Deactivating virtual environment..."
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
log_message "Checking existing Python version..."
python3 --version || log_error "No Python installation found."

log_message "Checking system Python..."
if command_exists python3; then
    log_message "Python is already installed on the system. Skipping Python installation."
else
    log_message "No Python found. Installing Python using Homebrew..."
    brew install python || handle_error $? "Failed to install Python"
fi

log_message "Verifying the installed Python version..."
python3 --version || handle_error $? "Python installation verification failed"
log_message "Python setup complete."

# Step 3: Update PATH in .zshrc
log_message "Checking if PATH update is needed in .zshrc..."
if ! grep -q 'export PATH="/opt/homebrew/bin:\$PATH"' ~/.zshrc; then
    log_message "Backing up current .zshrc..."
    cp ~/.zshrc ~/.zshrc.backup.$(date +%Y%m%d%H%M%S) || handle_error $? "Failed to backup .zshrc"
    
    log_message "Updating PATH in .zshrc..."
    echo 'export PATH="/opt/homebrew/bin:$PATH"' >> ~/.zshrc || handle_error $? "Failed to update .zshrc"
    log_message "Please restart your terminal or run 'source ~/.zshrc' manually for changes to take effect."
else
    log_message "PATH already contains Homebrew bin directory."
fi

# Step 4: Install pigz
log_message "Checking for pigz installation..."
if command_exists pigz; then
    log_message "pigz is already installed. Skipping installation."
else
    log_message "Installing pigz..."
    brew install pigz || handle_error $? "Failed to install pigz"
    log_message "pigz installation completed."
fi

# Step 5: Create and activate Python virtual environment
log_message "Creating virtual environment 'local_llms'..."
python3 -m venv local_llms || handle_error $? "Failed to create virtual environment"

log_message "Activating virtual environment..."
if [ -f "local_llms/bin/activate" ]; then
    source local_llms/bin/activate || handle_error $? "Failed to activate virtual environment"
else
    handle_error 1 "Virtual environment activation script not found."
fi
log_message "Virtual environment activated."

# Step 6: Install llama.cpp
log_message "Checking existing llama.cpp installation..."
if command -v llama-cli &>/dev/null; then
    log_message "llama.cpp is installed. Checking for updates..."
    if brew outdated | grep -q "llama.cpp"; then
        log_message "A newer version of llama.cpp is available. Upgrading..."
        brew upgrade llama.cpp || handle_error $? "Failed to upgrade llama.cpp"
        log_message "llama.cpp upgraded successfully."
    else
        log_message "llama.cpp is already at the latest version."
    fi
else
    log_message "No llama.cpp installation found. Installing..."
    brew install llama.cpp || handle_error $? "Failed to install llama.cpp"
    log_message "llama.cpp installation completed."
fi

log_message "Verifying the installed llama.cpp version..."
hash -r
llama-cli --version || handle_error $? "llama.cpp verification failed"
log_message "llama.cpp setup complete."

# Step 7: Set up local-llms toolkit
log_message "Setting up local-llms toolkit..."
# Check if local-llms is already installed
if pip3 show local-llms &>/dev/null; then
    log_message "local-llms is already installed. Checking for updates..."
    pip3 install -q --upgrade git+https://github.com/eternalai-org/local-llms.git || handle_error $? "Failed to update local-llms toolkit"
    log_message "local-llms toolkit is now up to date."
else
    log_message "Installing local-llms toolkit for the first time..."
    pip3 install -q git+https://github.com/eternalai-org/local-llms.git || handle_error $? "Failed to install local-llms toolkit"
    log_message "local-llms toolkit installed successfully."
fi

log_message "All steps completed successfully."