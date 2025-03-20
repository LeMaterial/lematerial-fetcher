import bz2
import gzip
import os
import re
import tempfile
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from lematerial_fetcher.utils.logging import logger


def create_session() -> requests.Session:
    """Create a session with retry capability."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def download_file(
    url: str,
    path: Optional[str] = None,
    desc: Optional[str] = None,
    decompress: str = None,
    position: int = 0,
) -> str:
    """
    Download a file from a URL and save it to a local path. Optionally decompress gzipped content on the fly.

    Parameters
    ----------
    url : str
        The URL to download the file from.
    path : Optional[str]
        The path to save the file to. If None, the file will be saved to a temporary directory.
    desc : Optional[str]
        The description to display in the progress bar.
    decompress : str
        The type of compression to decompress. If None, the file will not be decompressed.
        Supported types are "gz" and "bz2".
    position : int
        The position of the worker in the pool. Used to have different tqdm progress bars for different workers.

    Returns
    -------
    str
        The path to the downloaded file.
    """
    if path is None:
        # Create temporary directory if none provided
        temp_dir = tempfile.mkdtemp()
        os.makedirs(temp_dir, exist_ok=True)
        path = os.path.join(temp_dir, os.path.basename(url))

    session = create_session()

    response = session.get(url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    block_size = 8192

    if decompress == "gz":
        path = path.replace(".gz", "")
    elif decompress == "bz2":
        path = path.replace(".bz2", "")

    with open(path, "wb") as f:
        with tqdm(
            total=total_size, unit="iB", unit_scale=True, desc=desc, position=position
        ) as pbar:
            if decompress == "gz":
                decompressor = gzip.GzipFile(fileobj=response.raw, mode="rb")
                while True:
                    chunk = decompressor.read(block_size)
                    if not chunk:
                        break
                    size = f.write(chunk)
                    pbar.update(size)
            elif decompress == "bz2":
                decompressor = bz2.BZ2Decompressor()
                for chunk in response.iter_content(block_size):
                    if chunk:  # keep-alive chunks are ignored
                        try:
                            decompressed = decompressor.decompress(chunk)
                            size = f.write(decompressed)
                            pbar.update(size)
                        except EOFError:  # end of file
                            break
            else:
                for data in response.iter_content(block_size):
                    size = f.write(data)
                    pbar.update(size)

    return path


def list_download_links_from_page(
    url: str, pattern: str = None
) -> list[dict[str, str]]:
    """
    List all download links from an HTML page.

    If the page is an index page, the function will try to find the latest
    modified date and the size of the files. Otherwise, it will return the
    links from the page as is.

    Parameters
    ----------
    url : str
        The URL of the index page to parse
    pattern : str, optional
        Regex pattern to filter files. If None, all links are returned.

    Returns
    -------
    list[dict[str, str]]
        List of dictionaries containing:
        - url: The absolute URL of the file
        - filename: The filename
        - size: File size if available (or None)
        - last_modified: Last modification date if available (or None)
    """
    session = create_session()

    logger.info(f"Fetching index page: {url}")
    response = session.get(url, timeout=10)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    base_url = url

    # Compile regex pattern if provided
    if pattern:
        pattern = re.compile(pattern, re.IGNORECASE)

    download_links = []

    for link in soup.find_all("a", href=True):
        href = link["href"].strip()

        # Skip if it's a parent directory link
        if href in ("..", "../", "./"):
            continue

        # Make URL absolute
        absolute_url = urljoin(base_url, href)

        # Extract filename from URL
        filename = os.path.basename(urlparse(absolute_url).path)

        # Skip if pattern is provided and doesn't match
        if pattern and not pattern.search(filename):
            continue

        # We are trying to find size and last modified date
        size = None
        last_modified = None

        # Case 1: table cells format
        tr = link.find_parent("tr")
        if tr:
            # Common formats in index pages
            size_td = tr.find("td", text=re.compile(r"\d+[KMG]?B"))
            if size_td:
                size = size_td.text.strip()

            date_td = tr.find(
                "td", text=re.compile(r"\d{2}-[A-Za-z]{3}-\d{4}|\d{4}-\d{2}-\d{2}")
            )
            if date_td:
                date_str = date_td.text.strip()
                try:
                    last_modified = datetime.strptime(
                        date_str, "%d-%b-%Y %H:%M"
                    ).isoformat()
                except ValueError:
                    last_modified = date_str

        # Case 2: direct text node format (cf. Alexandria)
        if not (size and last_modified):  # Only try if we haven't found them yet
            next_text = link.next_sibling
            if next_text and isinstance(next_text, str):
                parts = next_text.strip().split()
                if len(parts) >= 3:
                    try:
                        date_str = f"{parts[0]} {parts[1]}"  # "21-Mar-2024 09:20"
                        last_modified = datetime.strptime(
                            date_str, "%d-%b-%Y %H:%M"
                        ).isoformat()
                        size = parts[2]
                    except (ValueError, IndexError):
                        logger.warning(f"Could not parse date/size from: {next_text}")

        download_links.append(
            {
                "url": absolute_url,
                "filename": filename,
                "size": size,
                "last_modified": last_modified,
            }
        )

    # Sort by filename
    download_links.sort(key=lambda x: x["filename"])

    if not download_links:
        logger.warning(f"No download links found at {url}")
    else:
        logger.info(f"Found {len(download_links)} download links")

    return download_links
