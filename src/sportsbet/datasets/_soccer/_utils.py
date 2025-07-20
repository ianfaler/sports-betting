"""Includes utilities for soccer data."""

# Author: Georgios Douzas <gdouzas@icloud.com>
# License: MIT

from __future__ import annotations

import asyncio
import io
import logging
from typing import Any

import aiohttp
import pandas as pd

OVER_UNDER = [1.5, 2.5, 3.5, 4.5]
OUTPUTS = [
    (
        'output__home_win__full_time_goals',
        lambda data: data['target__home_team__full_time_goals'] > data['target__away_team__full_time_goals'],
    ),
    (
        'output__away_win__full_time_goals',
        lambda data: data['target__home_team__full_time_goals'] < data['target__away_team__full_time_goals'],
    ),
    (
        'output__draw__full_time_goals',
        lambda data: data['target__home_team__full_time_goals'] == data['target__away_team__full_time_goals'],
    ),
    (
        f'output__over_{OVER_UNDER[0]}__full_time_goals',
        lambda data: data['target__home_team__full_time_goals'] + data['target__away_team__full_time_goals']
        > OVER_UNDER[0],
    ),
    (
        f'output__over_{OVER_UNDER[1]}__full_time_goals',
        lambda data: data['target__home_team__full_time_goals'] + data['target__away_team__full_time_goals']
        > OVER_UNDER[1],
    ),
    (
        f'output__over_{OVER_UNDER[2]}__full_time_goals',
        lambda data: data['target__home_team__full_time_goals'] + data['target__away_team__full_time_goals']
        > OVER_UNDER[2],
    ),
    (
        f'output__over_{OVER_UNDER[3]}__full_time_goals',
        lambda data: data['target__home_team__full_time_goals'] + data['target__away_team__full_time_goals']
        > OVER_UNDER[3],
    ),
    (
        f'output__under_{OVER_UNDER[0]}__full_time_goals',
        lambda data: data['target__home_team__full_time_goals'] + data['target__away_team__full_time_goals']
        < OVER_UNDER[0],
    ),
    (
        f'output__under_{OVER_UNDER[1]}__full_time_goals',
        lambda data: data['target__home_team__full_time_goals'] + data['target__away_team__full_time_goals']
        < OVER_UNDER[1],
    ),
    (
        f'output__under_{OVER_UNDER[2]}__full_time_goals',
        lambda data: data['target__home_team__full_time_goals'] + data['target__away_team__full_time_goals']
        < OVER_UNDER[2],
    ),
    (
        f'output__under_{OVER_UNDER[3]}__full_time_goals',
        lambda data: data['target__home_team__full_time_goals'] + data['target__away_team__full_time_goals']
        < OVER_UNDER[3],
    ),
]

# Enhanced connection configuration
CONNECTIONS_LIMIT = 10  # Reduced from 20 to be more conservative
REQUEST_TIMEOUT = 30  # Total timeout in seconds
CONNECT_TIMEOUT = 10  # Connection timeout in seconds
MAX_RETRIES = 3  # Maximum number of retry attempts
RETRY_DELAY = 1.0  # Initial delay between retries (exponential backoff)

# Setup logging
logger = logging.getLogger(__name__)


class NetworkError(Exception):
    """Custom exception for network-related errors."""
    pass


class DataValidationError(Exception):
    """Custom exception for data validation errors."""
    pass


async def _read_url_content_async_with_retry(
    client: aiohttp.ClientSession, 
    url: str, 
    max_retries: int = MAX_RETRIES
) -> str:
    """Read asynchronously the URL content with retry logic and proper error handling."""
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            async with client.get(url) as response:
                # Check if response is successful
                if response.status == 200:
                    content = await response.text(encoding='ISO-8859-1')
                    
                    # Basic validation - ensure we got some data
                    if not content or len(content.strip()) == 0:
                        raise DataValidationError(f"Empty response from {url}")
                    
                    # Additional validation - check if it looks like CSV data
                    if not any(delimiter in content[:1000] for delimiter in [',', '\t', ';']):
                        logger.warning(f"Response from {url} doesn't appear to be CSV data")
                    
                    return content
                else:
                    raise NetworkError(f"HTTP {response.status}: {response.reason} for {url}")
                    
        except asyncio.TimeoutError as e:
            last_exception = NetworkError(f"Timeout error for {url}: {e}")
            logger.warning(f"Attempt {attempt + 1} failed with timeout for {url}")
        except aiohttp.ClientError as e:
            last_exception = NetworkError(f"Client error for {url}: {e}")
            logger.warning(f"Attempt {attempt + 1} failed with client error for {url}: {e}")
        except DataValidationError as e:
            last_exception = e
            logger.error(f"Data validation failed for {url}: {e}")
            break  # Don't retry on validation errors
        except Exception as e:
            last_exception = NetworkError(f"Unexpected error for {url}: {e}")
            logger.error(f"Attempt {attempt + 1} failed with unexpected error for {url}: {e}")
        
        # If this wasn't the last attempt, wait before retrying
        if attempt < max_retries:
            delay = RETRY_DELAY * (2 ** attempt)  # Exponential backoff
            logger.info(f"Retrying {url} in {delay} seconds...")
            await asyncio.sleep(delay)
    
    # If we get here, all retries failed
    raise last_exception or NetworkError(f"Failed to fetch {url} after {max_retries + 1} attempts")


async def _read_url_content_async(client: aiohttp.ClientSession, url: str) -> str:
    """Read asynchronously the URL content."""
    return await _read_url_content_async_with_retry(client, url)


async def _read_urls_content_async(urls: list[str]) -> list[str]:
    """Read asynchronously the URLs content with enhanced error handling and timeouts."""
    if not urls:
        return []
    
    # Create timeout configuration
    timeout = aiohttp.ClientTimeout(
        total=REQUEST_TIMEOUT,
        connect=CONNECT_TIMEOUT,
        sock_read=REQUEST_TIMEOUT
    )
    
    # Create connector with connection limits
    connector = aiohttp.TCPConnector(
        limit=CONNECTIONS_LIMIT,
        limit_per_host=5,  # Limit per host to avoid overwhelming servers
        ttl_dns_cache=300,  # DNS cache TTL
        use_dns_cache=True,
    )
    
    try:
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={'User-Agent': 'SportsBot/1.0 (+https://github.com/georgedouzas/sports-betting)'}
        ) as client:
            tasks = [_read_url_content_async_with_retry(client, url) for url in urls]
            
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results and handle any exceptions
                processed_results = []
                failed_urls = []
                
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Failed to fetch {urls[i]}: {result}")
                        failed_urls.append(urls[i])
                        # For now, we'll skip failed URLs - could implement fallback logic here
                        continue
                    processed_results.append(result)
                
                if failed_urls:
                    logger.warning(f"Failed to fetch {len(failed_urls)} out of {len(urls)} URLs: {failed_urls}")
                    
                    # If too many URLs failed, raise an exception
                    if len(failed_urls) > len(urls) * 0.5:  # More than 50% failed
                        raise NetworkError(f"Too many URLs failed ({len(failed_urls)}/{len(urls)}). This might indicate a network or server issue.")
                
                return processed_results
                
            except Exception as e:
                logger.error(f"Error during batch URL fetching: {e}")
                raise NetworkError(f"Batch URL fetching failed: {e}") from e
                
    except Exception as e:
        logger.error(f"Error creating HTTP client session: {e}")
        raise NetworkError(f"Failed to create HTTP client: {e}") from e


def _read_urls_content(urls: list[str]) -> list[str]:
    """Read the URLs content with enhanced error handling."""
    try:
        return asyncio.run(_read_urls_content_async(urls))
    except Exception as e:
        logger.error(f"Failed to read URLs content: {e}")
        raise


def _read_csvs(urls: list[str]) -> list[pd.DataFrame]:
    """Read the CSVs with enhanced error handling and validation."""
    if not urls:
        return []
    
    try:
        urls_content = _read_urls_content(urls)
        csvs = []
        
        for i, content in enumerate(urls_content):
            try:
                # First, try to read just the header to get column names
                names = pd.read_csv(
                    io.StringIO(content), 
                    nrows=0, 
                    encoding='ISO-8859-1'
                ).columns.to_list()
                
                # Validate that we have some columns
                if not names:
                    logger.warning(f"No columns found in CSV from {urls[i] if i < len(urls) else 'unknown URL'}")
                    continue
                
                # Now read the full CSV
                csv = pd.read_csv(
                    io.StringIO(content), 
                    names=names, 
                    skiprows=1, 
                    encoding='ISO-8859-1', 
                    on_bad_lines='skip'
                )
                
                # Basic validation
                if csv.empty:
                    logger.warning(f"Empty CSV from {urls[i] if i < len(urls) else 'unknown URL'}")
                    continue
                
                # Log successful parsing
                logger.info(f"Successfully parsed CSV with {len(csv)} rows and {len(csv.columns)} columns")
                csvs.append(csv)
                
            except Exception as e:
                logger.error(f"Failed to parse CSV from {urls[i] if i < len(urls) else 'unknown URL'}: {e}")
                # Continue with other CSVs rather than failing completely
                continue
        
        if not csvs:
            raise DataValidationError("No valid CSV data could be parsed from any of the provided URLs")
        
        return csvs
        
    except Exception as e:
        logger.error(f"Failed to read CSVs: {e}")
        raise


def _read_csv(url: str) -> pd.DataFrame:
    """Read the CSV with enhanced error handling."""
    try:
        result = _read_csvs([url])
        if not result:
            raise DataValidationError(f"No valid CSV data from {url}")
        return result[0]
    except Exception as e:
        logger.error(f"Failed to read CSV from {url}: {e}")
        raise
