from cryptography.fernet import Fernet


def encrypt_config(file_name):
    # Generate a key (do this once and store it securely)
    key = Fernet.generate_key()
    with open("key.key", "wb") as key_file:
        key_file.write(key)

    # Encrypt the config file
    with open(f"{file_name}.json", "rb") as file:
        config_data = file.read()

    cipher = Fernet(key)
    encrypted_data = cipher.encrypt(config_data)

    # Save the encrypted data
    with open(f"{file_name}.json.enc", "wb") as enc_file:
        enc_file.write(encrypted_data)

    print("Configuration file encrypted successfully.")


if __name__ == "__main__":

    file_name = "config"
    
    encrypt_config(file_name)