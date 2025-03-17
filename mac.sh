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

# Step 1: Detect architecture and check Homebrew
if [[ $(uname -m) == "arm64" ]]; then
    HOMEBREW_PATH="/opt/homebrew/bin"
    log_message "Apple Silicon (M1/M2) Mac detected."
else
    HOMEBREW_PATH="/usr/local/bin"
    log_message "Intel Mac detected."
fi

if [ -x "$HOMEBREW_PATH/brew" ]; then
    export PATH="$HOMEBREW_PATH:$PATH"
else
    log_error "Homebrew not found. Please install Homebrew from https://brew.sh and run this script again."
    exit 1
fi

# Step 2: Determine which Python to use
if [ -x /usr/bin/python3 ]; then
    system_python_version=$(/usr/bin/python3 --version 2>&1 | awk '{print $2}')
    if [[ "$system_python_version" > "3.9" || "$system_python_version" == "3.9"* ]]; then
        log_message "System Python version is $system_python_version (>= 3.9). Using system Python."
        PYTHON_EXEC=/usr/bin/python3
    else
        log_message "System Python version is $system_python_version (< 3.9). Checking Homebrew's Python..."
        if [ -x "$HOMEBREW_PATH/python3" ]; then
            homebrew_python_version=$("$HOMEBREW_PATH/python3" --version 2>&1 | awk '{print $2}')
            if [[ "$homebrew_python_version" > "3.9" || "$homebrew_python_version" == "3.9"* ]]; then
                log_message "Homebrew's Python version is $homebrew_python_version (>= 3.9). Using Homebrew's Python."
                PYTHON_EXEC="$HOMEBREW_PATH/python3"
            else
                log_message "Homebrew's Python version is $homebrew_python_version (< 3.9). Upgrading Python via Homebrew..."
                brew upgrade python || handle_error $? "Failed to upgrade Python"
                PYTHON_EXEC="$HOMEBREW_PATH/python3"
            fi
        else
            log_message "No Homebrew Python found. Installing Python via Homebrew..."
            brew install python || handle_error $? "Failed to install Python"
            PYTHON_EXEC="$HOMEBREW_PATH/python3"
        fi
    fi
else
    log_message "No system Python found. Installing Python via Homebrew..."
    brew install python || handle_error $? "Failed to install Python"
    PYTHON_EXEC="$HOMEBREW_PATH/python3"
fi

log_message "Verifying the selected Python version..."
$PYTHON_EXEC --version || handle_error $? "Python verification failed"
log_message "Python setup complete."

# Step 3: Update PATH in .zshrc
log_message "Checking if Homebrew PATH update is needed in .zshrc..."
if ! grep -q "export PATH=\"$HOMEBREW_PATH:\$PATH\"" ~/.zshrc; then
    log_message "Backing up current .zshrc..."
    cp ~/.zshrc ~/.zshrc.backup.$(date +%Y%m%d%H%M%S) || handle_error $? "Failed to backup .zshrc"
    
    log_message "Updating PATH in .zshrc..."
    echo "export PATH=\"$HOMEBREW_PATH:\$PATH\"" >> ~/.zshrc || handle_error $? "Failed to update .zshrc"
    log_message "Please restart your terminal or run 'source ~/.zshrc' manually for changes to take effect."
else
    log_message "PATH already contains Homebrew bin directory for this architecture."
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
$PYTHON_EXEC -m venv local_llms || handle_error $? "Failed to create virtual environment"

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

if pip3 show local-llms &>/dev/null; then
    log_message "local-llms is already installed. Checking for updates..."
    pip3 install -q --upgrade git+https://github.com/eternalai-org/local-llms.git|| handle_error $? "Failed to update local-llms toolkit"
    log_message "local-llms toolkit is now up to date."
else
    log_message "Installing local-llms toolkit for the first time..."
    pip3 install -q git+https://github.com/eternalai-org/local-llms.git|| handle_error $? "Failed to install local-llms toolkit"
    log_message "local-llms toolkit installed successfully."
fi

log_message "All steps completed successfully."