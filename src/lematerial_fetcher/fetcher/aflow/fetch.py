import requests
import logging
from datetime import datetime
from typing import List, Any

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
        pass

    def get_new_version(self) -> str:
        return datetime.now().strftime("%Y-%m-%d")

    def get_items_to_process(self) -> ItemsInfo:
        logger.info("Querying AFLOW for total entry count...")
        # Minimal query to get count without crashing
        query = f"paging(0,0),format(json)"
        try:
            response = requests.get(self.API_URL + query, timeout=60)
            response.raise_for_status()
            meta = response.json()
            total_count = int(meta.get("results_count", 3500000))
        except Exception as e:
            logger.warning(f"Could not determine total count: {e}. Defaulting to unlimited.")
            total_count = None 
        return ItemsInfo(start_offset=0, total_count=total_count)

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
        print(f"\n[DEBUG] Requesting URL: {url}")

        entries = []
        try:
            response = requests.get(url, timeout=120)
            
            if response.status_code != 200:
                logger.error(f"Error {response.status_code} on page {page_number}")
                return False

            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError:
                print(f"\n[DEBUG] AFLOW Response Preview (Page {page_number}):")
                print(response.text[:1000])
                logger.error(f"AFLOW returned non-JSON. See stdout for details.")
                return False

            if isinstance(data, dict):
                entries = list(data.values()) 
                if "results_count" in data: 
                    pass 
            elif isinstance(data, list):
                entries = data
            
        except Exception as e:
            logger.error(f"Exception on page {page_number}: {e}")
            return False

        if not entries:
            return False

        try:
            db = StructuresDatabase(config.db_conn_str, config.table_name)
            formatted_data = [{"data": entry} for entry in entries]
            db.insert_data(formatted_data) 
            logger.info(f"[Worker {worker_id}] Saved {len(entries)} entries from Page {page_number}")
            return True
        except Exception as e:
            logger.error(f"Database error on page {page_number}: {e}")
            raise e
