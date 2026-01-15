import requests
import logging
from datetime import datetime
from typing import List, Any
import requests
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
    def get_session():
        """
        Creates a requests Session with automatic retry logic.
        This is done in case we request a page from AFLOW and it fails
        because of too many requests or a connection error, we don't want to
        stop our pipeline.
        Retries on: Connection errors, 500, 502, 503, 504, 429 (Too Many Requests).
        """
        retry_strategy = Retry(
            total=5,  # Retry 5 times
            backoff_factor=1,  # Wait 1s, 2s, 4s...
            status_forcelist=[429, 500, 502, 503, 504],
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
        try:
            # session.get will now auto-retry 5 times before raising an exception
            response = session.get(url, timeout=120)
            
            # If we still get a bad status after 5 retries, we log it.
            if response.status_code != 200:
                logger.error(f"Error {response.status_code} on page {page_number} after retries.")
                # IMPORTANT: If 404, maybe return False (end of data). 
                # If 500, we might want to return False to skip this page but keep going?
                # Usually, returning False stops the fetcher. 
                return False 

            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError:
                logger.error(f"Error decoding JSON on page {page_number}.")
                return False

            if isinstance(data, dict):
                entries = list(data.values()) 
            elif isinstance(data, list):
                entries = data
            
        except Exception as e:
            # This catches "Max Retries Exceeded"
            logger.error(f"Failed to fetch page {page_number} after retries: {e}")
            return False

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
                db.batch_insert_data(structures_to_save)
                logger.info(f"[Worker {worker_id}] Saved {len(structures_to_save)} entries from Page {page_number}")
            
            return True    
            
            
            # formatted_data = [{"data": entry} for entry in entries]
            # db.insert_data(formatted_data) 
            # logger.info(f"[Worker {worker_id}] Saved {len(entries)} entries from Page {page_number}")
            # return True
        except Exception as e:
            logger.error(f"Database error on page {page_number}: {e}")
            raise e
