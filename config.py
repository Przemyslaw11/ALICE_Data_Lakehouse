import os

from dotenv import load_dotenv
from logger import setup_logger

load_dotenv()

logger = setup_logger(__name__)

REQUIRED_ENV_VARS = [
    'AWS_ACCESS_KEY',
    'AWS_SECRET_KEY',
]


AWS_CONFIG = {
    'access_key': os.getenv('AWS_ACCESS_KEY'),
    'secret_key': os.getenv('AWS_SECRET_KEY'),
    'endpoint': os.getenv('AWS_ENDPOINT'),
    'region': os.getenv('AWS_REGION', 'us-west-2'),
}

