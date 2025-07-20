"""Memory management utilities for the sports betting GUI."""

import gc
import logging
import psutil
import threading
import time
from typing import Any, Dict, List
import pandas as pd

logger = logging.getLogger(__name__)

# Memory configuration
MAX_MEMORY_USAGE = 500 * 1024 * 1024  # 500MB limit
MEMORY_CHECK_INTERVAL = 30  # Check memory every 30 seconds
MEMORY_WARNING_THRESHOLD = 0.8  # Warn when 80% of limit is reached
MEMORY_CLEANUP_THRESHOLD = 0.9  # Clean up when 90% of limit is reached


class MemoryManager:
    """Manages memory usage for the application."""
    
    def __init__(self, max_memory: int = MAX_MEMORY_USAGE):
        self.max_memory = max_memory
        self.current_usage = 0
        self.tracked_objects: Dict[str, Any] = {}
        self.monitoring = False
        self.monitor_thread = None
        
    def start_monitoring(self):
        """Start memory monitoring in a background thread."""
        if not self.monitoring:
            self.monitoring = True
            self.monitor_thread = threading.Thread(target=self._monitor_memory, daemon=True)
            self.monitor_thread.start()
            logger.info("Memory monitoring started")
    
    def stop_monitoring(self):
        """Stop memory monitoring."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("Memory monitoring stopped")
    
    def _monitor_memory(self):
        """Monitor memory usage in background thread."""
        while self.monitoring:
            try:
                self._check_memory_usage()
                time.sleep(MEMORY_CHECK_INTERVAL)
            except Exception as e:
                logger.error(f"Error in memory monitoring: {e}")
                time.sleep(MEMORY_CHECK_INTERVAL)
    
    def _check_memory_usage(self):
        """Check current memory usage and take action if needed."""
        try:
            # Get process memory usage
            process = psutil.Process()
            memory_info = process.memory_info()
            current_memory = memory_info.rss  # Resident Set Size
            
            memory_percent = current_memory / self.max_memory
            
            if memory_percent >= MEMORY_CLEANUP_THRESHOLD:
                logger.warning(f"Memory usage critical: {current_memory:,} bytes ({memory_percent:.1%})")
                self.emergency_cleanup()
            elif memory_percent >= MEMORY_WARNING_THRESHOLD:
                logger.warning(f"Memory usage high: {current_memory:,} bytes ({memory_percent:.1%})")
                self.gentle_cleanup()
            
            self.current_usage = current_memory
            
        except Exception as e:
            logger.error(f"Failed to check memory usage: {e}")
    
    def track_object(self, name: str, obj: Any) -> bool:
        """Track a memory-consuming object."""
        try:
            obj_size = self._estimate_object_size(obj)
            
            # Check if adding this object would exceed memory limit
            if self.current_usage + obj_size > self.max_memory:
                logger.warning(f"Cannot track object '{name}': would exceed memory limit")
                return False
            
            self.tracked_objects[name] = {
                'object': obj,
                'size': obj_size,
                'timestamp': time.time()
            }
            
            logger.info(f"Tracking object '{name}' ({obj_size:,} bytes)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to track object '{name}': {e}")
            return False
    
    def untrack_object(self, name: str):
        """Stop tracking an object and remove it."""
        if name in self.tracked_objects:
            obj_info = self.tracked_objects.pop(name)
            logger.info(f"Untracked object '{name}' ({obj_info['size']:,} bytes)")
            # Force garbage collection
            del obj_info
            gc.collect()
    
    def _estimate_object_size(self, obj: Any) -> int:
        """Estimate the memory size of an object."""
        try:
            if isinstance(obj, pd.DataFrame):
                return obj.memory_usage(deep=True).sum()
            elif isinstance(obj, (list, tuple)):
                if len(obj) > 0:
                    # Estimate based on first few items
                    sample_size = min(10, len(obj))
                    sample_total = sum(len(str(obj[i]).encode('utf-8')) for i in range(sample_size))
                    return (sample_total // sample_size) * len(obj)
                return 0
            elif isinstance(obj, dict):
                return len(str(obj).encode('utf-8'))
            elif isinstance(obj, str):
                return len(obj.encode('utf-8'))
            else:
                # Fallback estimation
                return len(str(obj).encode('utf-8'))
        except Exception:
            # Conservative estimate if calculation fails
            return 1024 * 1024  # 1MB default
    
    def gentle_cleanup(self):
        """Perform gentle cleanup of old/large objects."""
        try:
            logger.info("Performing gentle memory cleanup")
            
            # Sort objects by age (oldest first)
            sorted_objects = sorted(
                self.tracked_objects.items(),
                key=lambda x: x[1]['timestamp']
            )
            
            # Remove oldest 25% of objects
            num_to_remove = max(1, len(sorted_objects) // 4)
            for name, _ in sorted_objects[:num_to_remove]:
                self.untrack_object(name)
            
            # Force garbage collection
            gc.collect()
            
            logger.info(f"Gentle cleanup completed: removed {num_to_remove} objects")
            
        except Exception as e:
            logger.error(f"Failed during gentle cleanup: {e}")
    
    def emergency_cleanup(self):
        """Perform aggressive cleanup when memory is critical."""
        try:
            logger.warning("Performing emergency memory cleanup")
            
            # Remove all but the most recent objects
            sorted_objects = sorted(
                self.tracked_objects.items(),
                key=lambda x: x[1]['timestamp'],
                reverse=True
            )
            
            # Keep only the 5 most recent objects
            objects_to_keep = sorted_objects[:5]
            objects_to_remove = sorted_objects[5:]
            
            for name, _ in objects_to_remove:
                self.untrack_object(name)
            
            # Force aggressive garbage collection
            gc.collect()
            gc.collect()  # Run twice for better cleanup
            
            logger.warning(f"Emergency cleanup completed: removed {len(objects_to_remove)} objects")
            
        except Exception as e:
            logger.error(f"Failed during emergency cleanup: {e}")
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """Get current memory statistics."""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            
            tracked_size = sum(obj['size'] for obj in self.tracked_objects.values())
            
            return {
                'current_usage': memory_info.rss,
                'max_memory': self.max_memory,
                'usage_percent': (memory_info.rss / self.max_memory) * 100,
                'tracked_objects': len(self.tracked_objects),
                'tracked_size': tracked_size,
                'free_memory': self.max_memory - memory_info.rss
            }
        except Exception as e:
            logger.error(f"Failed to get memory stats: {e}")
            return {}
    
    def cleanup_all(self):
        """Clean up all tracked objects."""
        logger.info("Cleaning up all tracked objects")
        object_names = list(self.tracked_objects.keys())
        for name in object_names:
            self.untrack_object(name)
        gc.collect()


# Global memory manager instance
memory_manager = MemoryManager()


def paginate_data(data: List[Any], page_size: int = 1000) -> List[List[Any]]:
    """Paginate large data for memory efficiency."""
    if not data:
        return []
    
    pages = []
    for i in range(0, len(data), page_size):
        pages.append(data[i:i + page_size])
    
    logger.info(f"Paginated data into {len(pages)} pages of size {page_size}")
    return pages


def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize DataFrame memory usage."""
    try:
        original_memory = df.memory_usage(deep=True).sum()
        
        # Optimize numeric columns
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')
        
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')
        
        # Optimize object columns
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].nunique() / len(df) < 0.5:  # If less than 50% unique values
                df[col] = df[col].astype('category')
        
        optimized_memory = df.memory_usage(deep=True).sum()
        reduction = (original_memory - optimized_memory) / original_memory
        
        logger.info(f"DataFrame memory optimized: {reduction:.1%} reduction "
                   f"({original_memory:,} -> {optimized_memory:,} bytes)")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to optimize DataFrame: {e}")
        return df


def check_memory_before_operation(required_memory: int) -> bool:
    """Check if there's enough memory for an operation."""
    try:
        stats = memory_manager.get_memory_stats()
        free_memory = stats.get('free_memory', 0)
        
        if required_memory > free_memory:
            logger.warning(f"Insufficient memory: need {required_memory:,}, "
                         f"available {free_memory:,}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to check memory: {e}")
        return False  # Err on the side of caution