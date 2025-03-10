from local_llms import __version__
from setuptools import setup, find_packages

setup(
    name="local_llms",
    version=__version__,
    packages=find_packages(),
    install_requires=[
        "requests",
        "tqdm",
        "loguru",
        "psutil",
        "httpx",
        "loguru"
    ],
    entry_points={
        "console_scripts": [
            "local-llms = local_llms.cli:main",
        ],
    },
    author="EternalAI",
    description="A library to manage local language models",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
)