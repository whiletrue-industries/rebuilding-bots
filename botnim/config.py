from pathlib import Path
import dotenv
import logging

ROOT = Path(__file__).parent.parent
SPECS = ROOT / 'specs'

dotenv.load_dotenv(ROOT / '.env')

# Logging configuration
logging.basicConfig(level=logging.INFO)
