import requests
import logging
from datetime import datetime
from typing import List, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.fetch import BaseFetcher, ItemsInfo, BatchInfo
from lematerial_fetcher.utils.config import FetcherConfig
from lematerial_fetcher.database.postgres import StructuresDatabase

logger = logging.getLogger(__name__)

class AflowFetcher(BaseFetcher):
    """
    Fetcher for AFLOW data using the AFLUX Search API.
    """
    
    API_URL = "https://aflow.org/API/aflux/?"
    
    KEYWORDS = [
        # --- Identifiers & Composition ---
        "auid",
        "compound", 
        "species", 
        "natoms", 
        "composition", 
        "species_pp_version",      # Pseudopotential info
        
        # --- Structure ---
        "geometry",               # Full geometry string
        "positions_cartesian",    # Cartesian positions
        "positions_fractional",   # Fractional positions
        "spacegroup_relax",       # Relaxed spacegroup info
        "aflow_prototype_label_relax", 
        
        # --- Energetics (Corrected Keys) ---
        "enthalpy_formation_atom", # Was 'enthalpy_atom' (incorrect)
        "energy_cell",
        "energy_cutoff",
        "dft_type", 
        "kpoints_relax", 
        
        # --- Metadata ---
        "aflowlib_date",          # Was 'aflowlib_entry_date' (incorrect)
        
        # --- Electronic / Magnetic ---
        "spin_cell", 
        "spin_atom",
        
        # --- Hubbard U (LDA+U) ---
        "ldau_type", 
        "ldau_l", 
        "ldau_u", 
        "ldau_j",

        # --- HEAVY FIELDS (Warning: High risk of 'DB Fail!null' timeouts) ---
        # Uncomment these only if you absolutely need them.
        "forces", 
        "stress_tensor", 
    ]

    def setup_resources(self) -> None:
        """Set up necessary resources."""
        logger.info("Setting up AFLOW fetcher resources")
        # This reads self.config.table_name (aflow_source) and creates it in Postgres
        self.setup_database()

    def get_new_version(self) -> str:
        return datetime.now().strftime("%Y-%m-%d")

    def get_items_to_process(self) -> ItemsInfo:
        """
        Returns ItemsInfo with total_count=None.
        This triggers 'Unlimited Mode' in BaseFetcher.
        """
        logger.info("Skipping global count (AFLOW API is unstable for counts).")
        logger.info("Fetcher will run in 'Unlimited Mode' until empty response.")
        
        # start_offset=0, total_count=None
        return ItemsInfo(start_offset=0, total_count=None)

    @staticmethod
    def get_session() -> requests.Session:
        """
        Creates a requests Session with aggressive retry logic.
        
        Config:
        - total=10: Tries 10 times before giving up.
        - backoff_factor=2: Pauses increasingly longer between tries.
          (Waits: 1s, 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s...)
          This gives the AFLOW server plenty of time to recover if it's overloaded.
        """
        retry_strategy = Retry(
            total=10, 
            backoff_factor=1, 
            # 429 = Too Many Requests (You are being rate limited)
            # 500, 502, 503, 504 = Server Crashes / Gateway Timeouts
            status_forcelist=[429, 500, 502, 503, 504],
            # Don't raise a MaxRetryError immediately for status codes, let us handle it
            raise_on_status=False, 
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    @staticmethod
    def _process_batch(
        batch: Any, config: FetcherConfig, manager_dict: dict, worker_id: int = 0
    ) -> bool:
        page_size = batch.limit
        page_number = (batch.offset // page_size) + 1
        
        logger.info(f"[Worker {worker_id}] Fetching Page {page_number} (Offset {batch.offset})...")

        query_args = [
            ",".join(AflowFetcher.KEYWORDS),
            f"paging({page_number},{page_size})",
            "format(json)"
        ]
        url = AflowFetcher.API_URL + ",".join(query_args)
        
        # DEBUG: Print the simplified URL
        logger.debug(f"\n[DEBUG] Requesting URL: {url}")

        entries = []

        session = AflowFetcher.get_session()
        # ... inside _process_batch ...
        
        try:
            # The adapter handles the retries/pauses automatically here.
            # If it fails 10 times on connection errors, it raises MaxRetryError/ConnectionError.
            response = session.get(url, timeout=120)
            
            # --- STATUS CODE CHECKING ---
            
            # 1. Success
            if response.status_code == 200:
                pass # Continue to processing

            # 2. End of Data (AFLOW sometimes returns 404 or empty JSON for end of list)
            elif response.status_code == 404:
                logger.info(f"Page {page_number} returned 404 (Not Found). Assuming end of dataset.")
                return False  # STOP the worker

            # 3. Client Error (400 Bad Request) - Retrying won't fix typos
            elif 400 <= response.status_code < 500:
                logger.error(f"Client Error {response.status_code} on page {page_number}. Skipping page.")
                return True   # SKIP this page, keep worker alive

            # 4. Server Error (500+) - We already retried 10 times via adapter!
            else:
                logger.error(f"Server Error {response.status_code} on page {page_number} after 10 retries. Skipping.")
                return True   # SKIP this page

            # --- JSON PARSING ---
            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError:
                logger.error(f"Error decoding JSON on page {page_number}. (Content might be HTML error page).")
                return True

            if isinstance(data, dict):
                entries = list(data.values()) 
            elif isinstance(data, list):
                entries = data
            
        except Exception as e:
            # This catches the final "Max Retries Exceeded" exception if the internet is down
            logger.error(f"FATAL: Failed to fetch page {page_number} after 10 retries: {e}")
            # Ensure we return True so the worker doesn't die. It will try the next page.
            return True
        

        if not entries:
            return False

        try:
            db = StructuresDatabase(config.db_conn_str, config.table_name)
        # 1. Convert dicts to RawStructure objects
            structures_to_save = []
            for entry in entries:
                # Use 'auid' as the ID, fallback to unknown if missing
                row_id = entry.get("auid", "unknown")
                
                # Wrap in RawStructure so the DB handler can read .attributes
                struct = RawStructure(
                    id=row_id,
                    type="structure",
                    attributes={"data": entry}, # This matches your Transformer logic
                    last_modified=datetime.now().isoformat()
                )
                structures_to_save.append(struct)

            # 2. Use batch_insert_data for lists
            if structures_to_save:
                # SANITIZATION STEP: Deduplicate structures based on ID
                # If the API sends the same ID twice, this dictionary comprehension
                # keeps only the LAST occurrence, effectively removing duplicates.
                unique_map = {s.id: s for s in structures_to_save}
                unique_structures = list(unique_map.values())

                if len(structures_to_save) != len(unique_structures):
                    # Optional: Log it so you know it's working
                    logger.warning(f"Removed {len(structures_to_save) - len(unique_structures)} duplicates from batch.")

                # Insert the clean, unique list
                if unique_structures:
                    db.batch_insert_data(unique_structures) 
                    logger.info(f"[Worker {worker_id}] Saved {len(unique_structures)} entries from Page {page_number}")
            return True    
            
            
            # formatted_data = [{"data": entry} for entry in entries]
            # db.insert_data(formatted_data) 
            # logger.info(f"[Worker {worker_id}] Saved {len(entries)} entries from Page {page_number}")
            # return True
        except Exception as e:
            logger.error(f"Database error on page {page_number}: {e}")
            raise e
