import sys
import argparse
from loguru import logger
from local_llms import __version__
from local_llms.core import LocalLLMManager

manager = LocalLLMManager()

def parse_args():
    parser = argparse.ArgumentParser(
        description="Tool for managing local large language models"
    )
    subparsers = parser.add_subparsers(
        dest='command', help="Commands for managing local language models"  
    )
    start_command = subparsers.add_parser(
        "start", help="Start a local language model server"
    )
    start_command.add_argument(
        "--hash", type=str, required=True,
        help="Filecoin hash of the model to start"
    )
    start_command.add_argument(
        "--port", type=int, default=8080,
        help="Port number for the local language model server"
    )
    start_command.add_argument(
        "--host", type=str, default="localhost",
        help="Host address for the local language model server"
    )
    stop_command = subparsers.add_parser(
        "stop", help="Stop a local language model server"
    )
    version_command = subparsers.add_parser(
        "version", help="Print the version of local_llms"
    )
    return parser.parse_known_args()

def version_command():
    logger.info(
        f"Local LLMS (Large Language Model Service) version: {__version__}"
    )

def handle_start(args):
    if not manager.start(args.hash, args.port):
        sys.exit(1)

def main():
    known_args, unknown_args = parse_args()
    for arg in unknown_args:
        logger.error(f'unknown command or argument: {arg}')
        sys.exit(2)

    if known_args.command == "version":
        version_command()
    elif known_args.command == "start":
        handle_start(known_args)


if __name__ == "__main__":
    main()
