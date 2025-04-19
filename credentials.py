# credentials.py

import os


def get_credentials():
    """
    Load credentials securely from environment variables or a config file.
    """
    user = os.getenv("SF_USER")
    password = os.getenv("SF_PASSWORD")
    account = os.getenv("SF_ACCOUNT")
    private_key_path = os.getenv("PRIVATE_KEY_PATH", "rsa_key.p8")

    private_key = None
    if os.path.exists(private_key_path):
        with open(private_key_path, "r") as key_file:
            private_key = key_file.read()

    return {
        "user": user,
        "password": password,
        "account": account,
        "private_key": private_key
    }
