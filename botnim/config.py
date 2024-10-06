from pathlib import Path
import dotenv

ROOT = Path(__file__).parent.parent
SPECS = ROOT / 'specs'

dotenv.load_dotenv(SPECS / '.env')
