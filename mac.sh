# Log function to print messages with timestamps
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Step 1: Install Python
log "Installing Python..."
brew install python
log "Python installation completed."

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
log "Installing llama.cpp..."
brew install llama.cpp
log "llama.cpp installation completed."

# Step 6: Set up local-llms toolkit
log "Setting up local-llms toolkit..."
pip3 install -e .
log "local-llms toolkit setup completed."

log "All steps completed successfully."