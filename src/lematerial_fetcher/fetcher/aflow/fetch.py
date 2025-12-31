"""Fetcher for AFLOW data using the AFLOW ALFUX API.

We tried to use the optimade API but failed running simple queries on the
AFLOW database so we use the native AFLUX API for which we were
sucessful getting 5000 rows of data per page.
"""
import requests
import logging
from typing import List, Dict, Any
from lematerial_fetcher.fetcher.base import BaseFetcher

logger = logging.getLogger(__name__)


class AflowFetcher(BaseFetcher):
    """
    Fetcher for AFLOW data using the AFLUX Search API.
    """
    
    API_URL = "http://aflow.org/API/aflux/?"
    # We explicitly list keywords to keep response size manageable and 
    # ensure we get the fields needed for the LeMatBulk dataset.
    # K points are not used in the HF dataset.
    KEYWORDS = [
        "auid",
        "compound", "geometry", "positions_cartesian", "species", "natoms"
        "aflow_prototype_label_relax", "composition", "spacegroup_relax",

        "energy_cell", "forces", "stress_tensor", "spin_cell", "spin_atom",

        

        "dft_type", "kpoints_relax", "aflowlib_entry_date", "energy_cutoff",
        # The Hubbard keys are only sometimes present.
        # ldau_type=2
        # ldau_l=[0, 2, 2]
        # ldau_u=[0, 2.1, 3]
        # ldau_j=[0, 0, 0]
        # ldau_TLUJ=[2, [0, 2, 2], [0, 2.1, 3], [0, 0, 0]] # captures all in one.
        "ldau_type", "ldau_l", "ldau_u", "ldau_j",
        # Pseudopotential version of the species: ['Cr_pv:PAW_PBE:07Sep2000']
        "species_pp_version"
    ]
    PAGE_SIZE = 5000

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = "raw_aflow"

    def get_tasks(self) -> List[int]:
        """
        Determines the total number of entries and returns a list of page numbers.
        """
        logger.info("Querying AFLOW for total entry count...")
        
        # AFLUX trick: paging(0) returns the total count in the response headers 
        # or a small response containing the total count 'N'.
        # We use a minimal query just to get the count.
        query = f"catalog(),paging(0,0),format(json)"
        response = requests.get(self.API_URL + query, timeout=60)
        response.raise_for_status()
        
        # AFLOW returns meta info in a dictionary if paging(0,0) is used
        meta = response.json()
        total_count = int(meta.get("results_count", 3000000)) # Fallback to ~3M if key missing
        
        total_pages = (total_count // self.PAGE_SIZE) + 1
        logger.info(f"Total entries: {total_count}, Total pages: {total_pages}")
        
        return list(range(1, total_pages + 1))

    def process_task(self, page_number: int) -> List[Dict[str, Any]]:
        """
        Fetches a single page of data from AFLOW.
        """
        query_args = [
            "catalog()",
            ",".join(self.KEYWORDS),
            f"paging({page_number},{self.PAGE_SIZE})",
            "format(json)"
        ]
        url = self.API_URL + ",".join(query_args)

        try:
            # AFLOW API can be flaky; use a timeout and handle exceptions
            response = requests.get(url, timeout=120)
            
            if response.status_code != 200:
                logger.error(f"Error {response.status_code} on page {page_number}")
                return []

            data = response.json()

            # Handle AFLOW's inconsistent return types (list vs dict)
            if isinstance(data, dict):
                entries = list(data.values())
            elif isinstance(data, list):
                entries = data
            else:
                entries = []

            # We return the raw data; the Transform step will handle the schema mapping
            return entries

        except Exception as e:
            logger.error(f"Exception on page {page_number}: {e}")
            # Returning an empty list allows the worker to finish 
            # without crashing the entire parallel process.
            return []
