import os
from pathlib import Path

from cryptography.hazmat.primitives.serialization import load_pem_private_key
from configparser import ConfigParser


def get_private_key(key_path: str):
    if not key_path:
        raise ValueError("Private key path is required")
    with Path(key_path).open("rb") as f:
        private_key_from_file = f.read()
    return load_pem_private_key(private_key_from_file, password=None)


def get_api_key(config_path: str):
    if not config_path:
        raise ValueError("Config path is required")
    config = ConfigParser()
    config.read(config_path)
    return config["keys"]["API_KEY"], config["keys"]["PATH_TO_PRIVATE_KEY_PEM_FILE"]
