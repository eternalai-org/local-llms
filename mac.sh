# Log function to print messages with timestamps
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Step 1: Install Python
log "Checking existing Python version..."
python3 --version || log "No Python installation found."

log "Installing/Updating Python to the latest version..."
brew install python || brew upgrade python
log "Python installation/update completed."

log "Verifying the installed Python version..."
python3 --version
log "Python setup complete."

# Step 2: Update PATH in .zshrc
log "Updating PATH in .zshrc..."
echo 'export PATH="/opt/homebrew/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
log "PATH updated successfully."

# Step 3: Install pigz
log "Installing pigz..."
brew install pigz
log "pigz installation completed."

# Step 4: Create and activate Python virtual environment
log "Creating virtual environment 'local_llms'..."
python3 -m venv local_llms
log "Activating virtual environment..."
source local_llms/bin/activate
log "Virtual environment activated."

# Step 5: Install llama.cpp
log "Checking existing llama.cpp installation..."
if command -v llama-cli &>/dev/null; then
    log "llama.cpp is installed. Checking for updates..."
else
    log "No llama.cpp installation found."
fi

log "Installing/Updating llama.cpp to the latest version..."
brew install llama.cpp || brew upgrade llama.cpp
log "llama.cpp installation/update completed."

log "Verifying the installed llama.cpp version..."
llama-cli --version || log "Verification command not found; installation may have issues."
log "llama.cpp setup complete."

# Step 6: Set up local-llms toolkit
log "Setting up local-llms toolkit..."
pip3 uninstall local-llms -y
pip3 install git+https://github.com/eternalai-org/local-llms.git
log "local-llms toolkit setup completed."

log "All steps completed successfully."