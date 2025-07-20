"""System configuration and integration for the sports betting application."""

import asyncio
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Import all our enhanced modules
from .cache_manager import cache_manager
from .database_manager import db_manager, initialize_database
from .error_handler import error_handler
from .memory_manager import memory_manager
from .monitoring import monitoring_system, start_monitoring, stop_monitoring
from .realtime_data import realtime_manager, initialize_realtime_data

logger = logging.getLogger(__name__)

# System configuration
SYSTEM_CONFIG = {
    'environment': os.getenv('SPORTSBET_ENV', 'development'),
    'log_level': os.getenv('SPORTSBET_LOG_LEVEL', 'INFO'),
    'database': {
        'type': os.getenv('SPORTSBET_DB_TYPE', 'sqlite'),
        'host': os.getenv('SPORTSBET_DB_HOST', 'localhost'),
        'port': int(os.getenv('SPORTSBET_DB_PORT', '5432')),
        'database': os.getenv('SPORTSBET_DB_NAME', 'sportsbet'),
        'user': os.getenv('SPORTSBET_DB_USER', 'sportsbet_user'),
        'password': os.getenv('SPORTSBET_DB_PASSWORD', 'sportsbet_pass'),
    },
    'cache': {
        'max_size': int(os.getenv('SPORTSBET_CACHE_SIZE', str(100 * 1024 * 1024))),  # 100MB
        'default_ttl': int(os.getenv('SPORTSBET_CACHE_TTL', '3600')),  # 1 hour
    },
    'memory': {
        'max_usage': int(os.getenv('SPORTSBET_MEMORY_LIMIT', str(500 * 1024 * 1024))),  # 500MB
        'monitoring_enabled': os.getenv('SPORTSBET_MEMORY_MONITORING', 'true').lower() == 'true',
    },
    'realtime': {
        'odds_api_key': os.getenv('SPORTSBET_ODDS_API_KEY'),
        'fixtures_api_key': os.getenv('SPORTSBET_FIXTURES_API_KEY'),
        'enabled': os.getenv('SPORTSBET_REALTIME_ENABLED', 'false').lower() == 'true',
    },
    'monitoring': {
        'enabled': os.getenv('SPORTSBET_MONITORING_ENABLED', 'true').lower() == 'true',
        'metrics_retention': int(os.getenv('SPORTSBET_METRICS_RETENTION', '3600')),  # 1 hour
    },
    'security': {
        'max_file_size': int(os.getenv('SPORTSBET_MAX_FILE_SIZE', str(50 * 1024 * 1024))),  # 50MB
        'allowed_origins': os.getenv('SPORTSBET_ALLOWED_ORIGINS', '*').split(','),
    }
}


class SystemManager:
    """Manages the entire sports betting application system."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or SYSTEM_CONFIG
        self.is_running = False
        self.shutdown_event = asyncio.Event()
        self.components_status = {
            'database': False,
            'cache': False,
            'memory_manager': False,
            'monitoring': False,
            'realtime_data': False
        }
    
    async def initialize(self):
        """Initialize all system components."""
        logger.info("🚀 Initializing Sports Betting Application System...")
        
        try:
            # Setup logging
            self._setup_logging()
            
            # Initialize database
            await self._initialize_database()
            
            # Initialize cache
            await self._initialize_cache()
            
            # Initialize memory management
            await self._initialize_memory_manager()
            
            # Initialize monitoring
            await self._initialize_monitoring()
            
            # Initialize real-time data (if enabled)
            if self.config['realtime']['enabled']:
                await self._initialize_realtime_data()
            
            logger.info("✅ System initialization completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ System initialization failed: {e}")
            await self.shutdown()
            raise
    
    async def start(self):
        """Start all system components."""
        if self.is_running:
            logger.warning("System is already running")
            return
        
        logger.info("🚀 Starting Sports Betting Application System...")
        
        try:
            # Start monitoring first
            if self.config['monitoring']['enabled']:
                start_monitoring()
                self.components_status['monitoring'] = True
                logger.info("✅ Monitoring system started")
            
            # Start memory management
            if self.config['memory']['monitoring_enabled']:
                memory_manager.start_monitoring()
                self.components_status['memory_manager'] = True
                logger.info("✅ Memory management started")
            
            # Start cache monitoring
            cache_manager.start_monitoring()
            self.components_status['cache'] = True
            logger.info("✅ Cache management started")
            
            # Start real-time data
            if self.config['realtime']['enabled'] and self.components_status.get('realtime_data'):
                await realtime_manager.start()
                logger.info("✅ Real-time data system started")
            
            self.is_running = True
            
            # Setup signal handlers for graceful shutdown
            self._setup_signal_handlers()
            
            logger.info("🎉 Sports Betting Application System is now running!")
            logger.info(f"Environment: {self.config['environment']}")
            logger.info(f"Database: {self.config['database']['type']}")
            logger.info(f"Real-time data: {'enabled' if self.config['realtime']['enabled'] else 'disabled'}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ System startup failed: {e}")
            await self.shutdown()
            raise
    
    async def shutdown(self):
        """Shutdown all system components gracefully."""
        if not self.is_running:
            return
        
        logger.info("🛑 Shutting down Sports Betting Application System...")
        
        try:
            # Stop real-time data
            if realtime_manager.is_running:
                await realtime_manager.stop()
                logger.info("✅ Real-time data system stopped")
            
            # Stop monitoring
            if self.components_status.get('monitoring'):
                stop_monitoring()
                self.components_status['monitoring'] = False
                logger.info("✅ Monitoring system stopped")
            
            # Stop memory management
            if self.components_status.get('memory_manager'):
                memory_manager.stop_monitoring()
                memory_manager.cleanup_all()
                self.components_status['memory_manager'] = False
                logger.info("✅ Memory management stopped")
            
            # Stop cache
            if self.components_status.get('cache'):
                cache_manager.stop_monitoring()
                self.components_status['cache'] = False
                logger.info("✅ Cache management stopped")
            
            # Close database
            if self.components_status.get('database'):
                await db_manager.close()
                self.components_status['database'] = False
                logger.info("✅ Database connection closed")
            
            self.is_running = False
            self.shutdown_event.set()
            
            logger.info("✅ System shutdown completed")
            
        except Exception as e:
            logger.error(f"❌ Error during system shutdown: {e}")
    
    def _setup_logging(self):
        """Setup application logging."""
        log_level = getattr(logging, self.config['log_level'].upper(), logging.INFO)
        
        # Create logs directory
        logs_dir = Path('logs')
        logs_dir.mkdir(exist_ok=True)
        
        # Configure logging
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(logs_dir / 'sportsbet.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        # Set third-party logging levels
        logging.getLogger('aiohttp').setLevel(logging.WARNING)
        logging.getLogger('websockets').setLevel(logging.WARNING)
        
        logger.info(f"✅ Logging configured (level: {self.config['log_level']})")
    
    async def _initialize_database(self):
        """Initialize database system."""
        try:
            db_config = self.config['database'].copy()
            db_type = db_config.pop('type')
            
            await initialize_database(db_type, db_config)
            self.components_status['database'] = True
            logger.info(f"✅ Database initialized ({db_type})")
            
        except Exception as e:
            logger.error(f"❌ Database initialization failed: {e}")
            raise
    
    async def _initialize_cache(self):
        """Initialize cache system."""
        try:
            # Cache is already initialized globally, just configure it
            cache_manager.max_size = self.config['cache']['max_size']
            self.components_status['cache'] = True
            logger.info("✅ Cache system configured")
            
        except Exception as e:
            logger.error(f"❌ Cache initialization failed: {e}")
            raise
    
    async def _initialize_memory_manager(self):
        """Initialize memory management system."""
        try:
            memory_manager.max_memory = self.config['memory']['max_usage']
            self.components_status['memory_manager'] = True
            logger.info("✅ Memory management configured")
            
        except Exception as e:
            logger.error(f"❌ Memory manager initialization failed: {e}")
            raise
    
    async def _initialize_monitoring(self):
        """Initialize monitoring system."""
        try:
            if self.config['monitoring']['enabled']:
                # Monitoring system is already initialized globally
                self.components_status['monitoring'] = True
                logger.info("✅ Monitoring system configured")
            else:
                logger.info("ℹ️ Monitoring system disabled")
                
        except Exception as e:
            logger.error(f"❌ Monitoring initialization failed: {e}")
            raise
    
    async def _initialize_realtime_data(self):
        """Initialize real-time data system."""
        try:
            realtime_config = {
                'odds_api': {
                    'api_key': self.config['realtime']['odds_api_key'],
                    'base_url': 'https://api.the-odds-api.com/v4',
                    'update_interval': 30,
                    'sports': ['soccer_epl', 'soccer_spain_la_liga', 'soccer_italy_serie_a']
                },
                'fixtures_api': {
                    'api_key': self.config['realtime']['fixtures_api_key'],
                    'base_url': 'https://api.football-data.org/v4',
                    'update_interval': 300
                }
            }
            
            await initialize_realtime_data(realtime_config)
            self.components_status['realtime_data'] = True
            logger.info("✅ Real-time data system initialized")
            
        except Exception as e:
            logger.error(f"❌ Real-time data initialization failed: {e}")
            raise
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            asyncio.create_task(self.shutdown())
        
        # Register signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        if hasattr(signal, 'SIGHUP'):
            signal.signal(signal.SIGHUP, signal_handler)
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive system health check."""
        health_status = {
            'overall_healthy': True,
            'components': {},
            'timestamp': None
        }
        
        try:
            # Database health
            if self.components_status.get('database'):
                db_health = await db_manager.health_check()
                health_status['components']['database'] = db_health
                if db_health.get('status') != 'healthy':
                    health_status['overall_healthy'] = False
            
            # Cache health
            if self.components_status.get('cache'):
                cache_stats = cache_manager.get_stats()
                health_status['components']['cache'] = {
                    'status': 'healthy' if cache_stats.get('usage_percent', 0) < 90 else 'warning',
                    'usage_percent': cache_stats.get('usage_percent', 0),
                    'entries': cache_stats.get('entry_count', 0)
                }
            
            # Memory health
            if self.components_status.get('memory_manager'):
                memory_stats = memory_manager.get_memory_stats()
                health_status['components']['memory'] = {
                    'status': 'healthy' if memory_stats.get('usage_percent', 0) < 90 else 'warning',
                    'usage_percent': memory_stats.get('usage_percent', 0),
                    'tracked_objects': memory_stats.get('tracked_objects', 0)
                }
            
            # Monitoring health
            if self.components_status.get('monitoring'):
                monitoring_data = monitoring_system.get_dashboard_data()
                health_status['components']['monitoring'] = {
                    'status': 'healthy' if monitoring_data['health_status']['overall_healthy'] else 'unhealthy',
                    'active_alerts': len(monitoring_data['active_alerts']),
                    'metrics_count': len(monitoring_data['metric_names'])
                }
            
            # Real-time data health
            if self.components_status.get('realtime_data'):
                realtime_stats = realtime_manager.get_stats()
                health_status['components']['realtime'] = {
                    'status': 'healthy' if realtime_stats['is_running'] else 'unhealthy',
                    'providers': realtime_stats['active_providers'],
                    'total_updates': realtime_stats['total_updates']
                }
            
            # Overall system health
            component_issues = [
                name for name, status in health_status['components'].items()
                if status.get('status') in ['unhealthy', 'critical']
            ]
            
            if component_issues:
                health_status['overall_healthy'] = False
                health_status['issues'] = component_issues
            
            health_status['timestamp'] = db_manager._get_current_timestamp() if hasattr(db_manager, '_get_current_timestamp') else None
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            health_status['overall_healthy'] = False
            health_status['error'] = str(e)
        
        return health_status
    
    def get_system_info(self) -> Dict[str, Any]:
        """Get comprehensive system information."""
        return {
            'environment': self.config['environment'],
            'is_running': self.is_running,
            'components_status': self.components_status,
            'configuration': {
                'database_type': self.config['database']['type'],
                'cache_size_mb': self.config['cache']['max_size'] // (1024 * 1024),
                'memory_limit_mb': self.config['memory']['max_usage'] // (1024 * 1024),
                'realtime_enabled': self.config['realtime']['enabled'],
                'monitoring_enabled': self.config['monitoring']['enabled']
            },
            'version': '1.0.0',  # You can read this from a version file
            'uptime_seconds': None  # Could calculate this if needed
        }
    
    async def wait_for_shutdown(self):
        """Wait for shutdown signal."""
        await self.shutdown_event.wait()


# Global system manager
system_manager = SystemManager()


async def initialize_system(config: Dict[str, Any] = None):
    """Initialize the global system manager."""
    global system_manager
    if config:
        system_manager.config.update(config)
    
    await system_manager.initialize()
    return system_manager


async def start_system():
    """Start the global system manager."""
    await system_manager.start()


async def shutdown_system():
    """Shutdown the global system manager."""
    await system_manager.shutdown()


async def system_health_check():
    """Perform system health check."""
    return await system_manager.health_check()


def get_system_info():
    """Get system information."""
    return system_manager.get_system_info()


# Application entry point
async def main():
    """Main application entry point."""
    try:
        # Initialize system
        await initialize_system()
        
        # Start system
        await start_system()
        
        # Wait for shutdown signal
        await system_manager.wait_for_shutdown()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        await shutdown_system()


if __name__ == "__main__":
    # Run the application
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nApplication interrupted by user")
    except Exception as e:
        print(f"Application failed: {e}")
        sys.exit(1)