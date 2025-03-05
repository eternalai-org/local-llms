# Local LLMs Toolkit: Easy Deployment of LLMs on Your Local Machine

The **Local LLMs Toolkit** is designed to simplify the process of deploying large language models (LLMs) locally on your machine. It includes an easy setup guide and ready-to-use scripts for a streamlined experience.

## Setup Instructions

### MacOS Installation

To get started, download the setup script and run it:
```bash
bash mac.sh
```
### Verify Installation

After installation, you can verify that everything was set up correctly by checking the version of the toolkit:
```bash
source local_llms/bin/activate
local-llms version
```
If you see the version number output, the installation is successful and you’re ready to start using Local LLMs Toolkit on your machine!

## Usage

### Download model from Filecoin
To download a model from Filecoin, run the following command:
```bash
local-llms download --hash <filecoin_hash>
```

### Start a Local LLM
To start a local LLM, run the following command:
```bash
local-llms start --hash <filecoin_hash>
```
Replace `<filecoin_hash>` with the cid of the LLM you want to start. For example:

```bash
local-llms start --hash bafkreiecx5ojce2tceibd74e2koniii3iweavknfnjdfqs6ows2ikoow6m
```

### Stop a Local LLM
To stop a local LLM, run the following command:
```bash
local-llms stop
```