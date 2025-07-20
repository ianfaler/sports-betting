"""Data caching system for the sports betting GUI."""

import hashlib
import json
import logging
import pickle
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd

logger = logging.getLogger(__name__)

# Cache configuration
CACHE_DIR = Path(".cache")
CACHE_DB = CACHE_DIR / "sportsbet_cache.db"
DEFAULT_TTL = 3600  # 1 hour default TTL
MAX_CACHE_SIZE = 100 * 1024 * 1024  # 100MB max cache size
CLEANUP_INTERVAL = 300  # Clean up every 5 minutes


class CacheManager:
    """Manages data caching with SQLite backend and TTL support."""
    
    def __init__(self, cache_dir: Path = CACHE_DIR, max_size: int = MAX_CACHE_SIZE):
        self.cache_dir = cache_dir
        self.max_size = max_size
        self.cache_dir.mkdir(exist_ok=True)
        self.db_path = self.cache_dir / "cache.db"
        self._lock = threading.RLock()
        self.monitoring = False
        self.monitor_thread = None
        
        # Initialize database
        self._init_database()
        
    def _init_database(self):
        """Initialize the cache database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS cache_entries (
                        key TEXT PRIMARY KEY,
                        data BLOB,
                        created_at REAL,
                        expires_at REAL,
                        size INTEGER,
                        access_count INTEGER DEFAULT 0,
                        last_accessed REAL
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_expires_at ON cache_entries(expires_at)
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_last_accessed ON cache_entries(last_accessed)
                """)
                conn.commit()
                logger.info("Cache database initialized")
        except Exception as e:
            logger.error(f"Failed to initialize cache database: {e}")
    
    def start_monitoring(self):
        """Start background monitoring and cleanup."""
        if not self.monitoring:
            self.monitoring = True
            self.monitor_thread = threading.Thread(target=self._monitor_cache, daemon=True)
            self.monitor_thread.start()
            logger.info("Cache monitoring started")
    
    def stop_monitoring(self):
        """Stop background monitoring."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("Cache monitoring stopped")
    
    def _monitor_cache(self):
        """Monitor cache and perform cleanup."""
        while self.monitoring:
            try:
                self.cleanup_expired()
                self.enforce_size_limit()
                time.sleep(CLEANUP_INTERVAL)
            except Exception as e:
                logger.error(f"Error in cache monitoring: {e}")
                time.sleep(CLEANUP_INTERVAL)
    
    def _generate_key(self, base_key: str, params: Dict[str, Any] = None) -> str:
        """Generate a unique cache key."""
        if params:
            # Sort parameters for consistent key generation
            sorted_params = json.dumps(params, sort_keys=True)
            key_data = f"{base_key}:{sorted_params}"
        else:
            key_data = base_key
        
        # Use SHA256 hash for consistent, safe keys
        return hashlib.sha256(key_data.encode()).hexdigest()
    
    def get(self, key: str, params: Dict[str, Any] = None) -> Optional[Any]:
        """Get data from cache."""
        cache_key = self._generate_key(key, params)
        
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute("""
                        SELECT data, expires_at, access_count 
                        FROM cache_entries 
                        WHERE key = ? AND expires_at > ?
                    """, (cache_key, time.time()))
                    
                    row = cursor.fetchone()
                    if row is None:
                        logger.debug(f"Cache miss for key: {key}")
                        return None
                    
                    data_blob, expires_at, access_count = row
                    
                    # Update access statistics
                    conn.execute("""
                        UPDATE cache_entries 
                        SET access_count = ?, last_accessed = ?
                        WHERE key = ?
                    """, (access_count + 1, time.time(), cache_key))
                    conn.commit()
                    
                    # Deserialize data
                    data = pickle.loads(data_blob)
                    logger.debug(f"Cache hit for key: {key}")
                    return data
                    
            except Exception as e:
                logger.error(f"Failed to get cached data for key {key}: {e}")
                return None
    
    def set(self, key: str, data: Any, ttl: int = DEFAULT_TTL, params: Dict[str, Any] = None) -> bool:
        """Set data in cache with TTL."""
        cache_key = self._generate_key(key, params)
        
        with self._lock:
            try:
                # Serialize data
                data_blob = pickle.dumps(data)
                data_size = len(data_blob)
                
                # Check if data is too large
                if data_size > self.max_size * 0.1:  # Don't cache items larger than 10% of max size
                    logger.warning(f"Data too large to cache: {data_size:,} bytes")
                    return False
                
                current_time = time.time()
                expires_at = current_time + ttl
                
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("""
                        INSERT OR REPLACE INTO cache_entries 
                        (key, data, created_at, expires_at, size, access_count, last_accessed)
                        VALUES (?, ?, ?, ?, ?, 0, ?)
                    """, (cache_key, data_blob, current_time, expires_at, data_size, current_time))
                    conn.commit()
                
                logger.debug(f"Cached data for key: {key} (size: {data_size:,} bytes, TTL: {ttl}s)")
                return True
                
            except Exception as e:
                logger.error(f"Failed to cache data for key {key}: {e}")
                return False
    
    def delete(self, key: str, params: Dict[str, Any] = None) -> bool:
        """Delete data from cache."""
        cache_key = self._generate_key(key, params)
        
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute("DELETE FROM cache_entries WHERE key = ?", (cache_key,))
                    conn.commit()
                    
                    if cursor.rowcount > 0:
                        logger.debug(f"Deleted cached data for key: {key}")
                        return True
                    else:
                        logger.debug(f"No cached data found for key: {key}")
                        return False
                        
            except Exception as e:
                logger.error(f"Failed to delete cached data for key {key}: {e}")
                return False
    
    def cleanup_expired(self):
        """Remove expired cache entries."""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute("""
                        DELETE FROM cache_entries 
                        WHERE expires_at <= ?
                    """, (time.time(),))
                    conn.commit()
                    
                    if cursor.rowcount > 0:
                        logger.info(f"Cleaned up {cursor.rowcount} expired cache entries")
                        
            except Exception as e:
                logger.error(f"Failed to cleanup expired cache entries: {e}")
    
    def enforce_size_limit(self):
        """Enforce cache size limit by removing least recently used items."""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    # Check current cache size
                    cursor = conn.execute("SELECT SUM(size) FROM cache_entries")
                    total_size = cursor.fetchone()[0] or 0
                    
                    if total_size <= self.max_size:
                        return
                    
                    logger.warning(f"Cache size {total_size:,} exceeds limit {self.max_size:,}")
                    
                    # Remove least recently used items until under limit
                    target_size = self.max_size * 0.8  # Reduce to 80% of limit
                    
                    cursor = conn.execute("""
                        SELECT key, size FROM cache_entries 
                        ORDER BY last_accessed ASC
                    """)
                    
                    removed_count = 0
                    for key, size in cursor:
                        if total_size <= target_size:
                            break
                        
                        conn.execute("DELETE FROM cache_entries WHERE key = ?", (key,))
                        total_size -= size
                        removed_count += 1
                    
                    conn.commit()
                    logger.info(f"Removed {removed_count} cache entries to enforce size limit")
                    
            except Exception as e:
                logger.error(f"Failed to enforce cache size limit: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute("""
                        SELECT 
                            COUNT(*) as entry_count,
                            SUM(size) as total_size,
                            AVG(access_count) as avg_access_count,
                            COUNT(CASE WHEN expires_at > ? THEN 1 END) as valid_entries
                        FROM cache_entries
                    """, (time.time(),))
                    
                    row = cursor.fetchone()
                    
                    return {
                        'entry_count': row[0] or 0,
                        'total_size': row[1] or 0,
                        'avg_access_count': row[2] or 0,
                        'valid_entries': row[3] or 0,
                        'max_size': self.max_size,
                        'usage_percent': ((row[1] or 0) / self.max_size) * 100
                    }
                    
            except Exception as e:
                logger.error(f"Failed to get cache stats: {e}")
                return {}
    
    def clear_all(self):
        """Clear all cache entries."""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("DELETE FROM cache_entries")
                    conn.commit()
                    logger.info("Cleared all cache entries")
            except Exception as e:
                logger.error(f"Failed to clear cache: {e}")


# Global cache manager instance
cache_manager = CacheManager()


def cached_data_loader(
    key: str, 
    loader_func, 
    ttl: int = DEFAULT_TTL, 
    params: Dict[str, Any] = None,
    force_refresh: bool = False
) -> Any:
    """Decorator/wrapper for caching data loading operations."""
    
    # Check cache first unless force refresh
    if not force_refresh:
        cached_data = cache_manager.get(key, params)
        if cached_data is not None:
            logger.info(f"Using cached data for: {key}")
            return cached_data
    
    # Load fresh data
    try:
        logger.info(f"Loading fresh data for: {key}")
        data = loader_func()
        
        # Cache the result
        cache_manager.set(key, data, ttl, params)
        
        return data
        
    except Exception as e:
        logger.error(f"Failed to load data for {key}: {e}")
        # Try to return stale cache data as fallback
        cached_data = cache_manager.get(key, params)
        if cached_data is not None:
            logger.warning(f"Using stale cached data for: {key}")
            return cached_data
        
        raise


def cache_dataframe(df: pd.DataFrame, key: str, ttl: int = DEFAULT_TTL) -> bool:
    """Cache a DataFrame with optimization."""
    try:
        # Optimize DataFrame before caching
        from .memory_manager import optimize_dataframe
        optimized_df = optimize_dataframe(df.copy())
        
        return cache_manager.set(key, optimized_df, ttl)
        
    except Exception as e:
        logger.error(f"Failed to cache DataFrame: {e}")
        return False


def get_cached_dataframe(key: str) -> Optional[pd.DataFrame]:
    """Get a cached DataFrame."""
    return cache_manager.get(key)


# Initialize cache manager on import
cache_manager.start_monitoring()