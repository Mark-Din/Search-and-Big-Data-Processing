import json
import os
from cryptography.fernet import Fernet
from logger import init_log

logger = init_log("decrypt")

def decrypt_config(file_name, script_dir):

    key_file_path = os.path.join(script_dir, "key.key")
    # Load the encryption key (store this key securely and never hardcode it)
    with open(key_file_path, "rb") as key_file:
        key = key_file.read()

    cipher = Fernet(key)

    enc_file_path = os.path.join(script_dir, f"{file_name}.json.enc")
    # Decrypt the configuration file
    with open(enc_file_path, "rb") as enc_file:
        encrypted_data = enc_file.read()

    config_data = cipher.decrypt(encrypted_data)

    # Parse the configuration as JSON
    config = json.loads(config_data)

    json_file_path = os.path.join(script_dir, f"{file_name}.json")
    with open(json_file_path, "w") as config_file:
        json.dump(config, config_file, indent=4)

    if not config:
        raise ValueError("Failed to load configuration.")
    
    return config


def get_config(script_dir):
    try:
        config = decrypt_config("config_concord", script_dir)
    except Exception as e:
        config = decrypt_config("config", script_dir)

    return config


if __name__ == "__main__":

    file_name = "config"

    script_dir = os.path.dirname(os.path.abspath(__file__))
    decrypt_config(file_name, script_dir)