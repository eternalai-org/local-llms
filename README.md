# Local LLMs Toolkit

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> Deploy and manage large language models locally with minimal setup.

## 📋 Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)

## 🔭 Overview

The **Local LLMs Toolkit** empowers developers to deploy state-of-the-art large language models directly on their machines. Bypass cloud services, maintain privacy, and reduce costs while leveraging powerful AI capabilities.

## ✨ Features

- **Simple Deployment**: Get models running with minimal configuration
- **Filecoin Integration**: Download models directly from the Filecoin network
- **Resource Management**: Automatic optimization for your hardware
- **Easy Monitoring**: Tools to track model status and performance

## 📦 Installation

### MacOS

```bash
bash mac.sh
```

### Verification

Confirm successful installation:

```bash
source local_llms/bin/activate
local-llms version
```

## 🚀 Usage

### Managing Models

```bash
# Check if model is available locally
local-llms check --hash <filecoin_hash>

# Download a model from Filecoin
local-llms download --hash <filecoin_hash>

# Start a model
local-llms start --hash <filecoin_hash>

# Example
local-llms start --hash bafkreiecx5ojce2tceibd74e2koniii3iweavknfnjdfqs6ows2ikoow6m

# Check running models
local-llms status

# Stop the current model
local-llms stop
```

## 👥 Contributing

Contributions welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.