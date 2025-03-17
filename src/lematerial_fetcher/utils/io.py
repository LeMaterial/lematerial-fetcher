import gzip
import os
import re
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
    path: str,
    session: requests.Session | None = None,
    desc: Optional[str] = None,
    decompress_gzip: bool = False,
) -> str:
    """
    Download a file from a URL and save it to a local path. Optionally decompress gzipped content on the fly.

    Parameters
    ----------
    url : str
        The URL to download the file from.
    path : str
        The path to save the file to.
    session : requests.Session | None
        The session to use for the download. If None, a new session will be created.
    desc : Optional[str]
        The description to display in the progress bar.
    decompress_gzip : bool
        Whether to decompress gzipped content while downloading. Default is False.

    Returns
    -------
    str
        The path to the downloaded file.
    """
    if session is None:
        session = create_session()

    response = session.get(url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    block_size = 8192

    if decompress_gzip:
        path = path.replace(".gz", "")

    with open(path, "wb") as f:
        with tqdm(total=total_size, unit="iB", unit_scale=True, desc=desc) as pbar:
            if decompress_gzip:
                decompressor = gzip.GzipFile(fileobj=response.raw, mode="rb")
                while True:
                    chunk = decompressor.read(block_size)
                    if not chunk:
                        break
                    size = f.write(chunk)
                    pbar.update(size)
            else:
                for data in response.iter_content(block_size):
                    size = f.write(data)
                    pbar.update(size)

    return path


def list_download_links_from_index_page(
    url: str, session: Optional[requests.Session] = None, pattern: str = None
) -> list[dict[str, str]]:
    """
    List all download links from an HTML index page.

    Parameters
    ----------
    url : str
        The URL of the index page to parse
    session : Optional[requests.Session]
        An existing session to use for the request. If None, creates a new one.
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
    if session is None:
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

        # Try to find size and last modified date
        # Look for parent tr element that might contain this info
        tr = link.find_parent("tr")
        size = None
        last_modified = None

        if tr:
            # Common formats in index pages
            size_td = tr.find("td", text=re.compile(r"\d+[KMG]?B"))
            if size_td:
                size = size_td.text.strip()

            date_td = tr.find(
                "td", text=re.compile(r"\d{4}-\d{2}-\d{2}|\d{2}-[A-Za-z]{3}-\d{4}")
            )
            if date_td:
                last_modified = date_td.text.strip()

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
