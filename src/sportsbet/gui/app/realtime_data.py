"""Real-time data integration system for live sports data and odds."""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set
import aiohttp
import websockets
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)

# Real-time data configuration
REALTIME_CONFIG = {
    'odds_api': {
        'base_url': 'https://api.the-odds-api.com/v4',
        'websocket_url': 'wss://api.the-odds-api.com/v4/ws',
        'api_key': None,  # Set via environment variable
        'update_interval': 30,  # seconds
        'sports': ['soccer_epl', 'soccer_spain_la_liga', 'soccer_italy_serie_a', 
                  'soccer_germany_bundesliga', 'soccer_france_ligue_one']
    },
    'fixtures_api': {
        'base_url': 'https://api.football-data.org/v4',
        'api_key': None,  # Set via environment variable
        'update_interval': 300,  # 5 minutes
    },
    'websocket': {
        'max_connections': 10,
        'heartbeat_interval': 30,
        'reconnect_delay': 5,
        'max_reconnects': 5
    }
}


class DataSourceType(Enum):
    """Types of real-time data sources."""
    ODDS = "odds"
    FIXTURES = "fixtures"
    LIVE_SCORES = "live_scores"
    STATISTICS = "statistics"


class DataUpdateType(Enum):
    """Types of data updates."""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


@dataclass
class RealTimeUpdate:
    """Represents a real-time data update."""
    source_type: DataSourceType
    update_type: DataUpdateType
    data: Dict[str, Any]
    timestamp: datetime
    match_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'source_type': self.source_type.value,
            'update_type': self.update_type.value,
            'data': self.data,
            'timestamp': self.timestamp.isoformat(),
            'match_id': self.match_id
        }


class RealTimeDataProvider:
    """Base class for real-time data providers."""
    
    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.config = config
        self.is_active = False
        self.last_update = None
        self.error_count = 0
        self.subscribers: Set[Callable] = set()
    
    async def start(self):
        """Start the data provider."""
        self.is_active = True
        logger.info(f"Started real-time data provider: {self.name}")
    
    async def stop(self):
        """Stop the data provider."""
        self.is_active = False
        logger.info(f"Stopped real-time data provider: {self.name}")
    
    def subscribe(self, callback: Callable[[RealTimeUpdate], None]):
        """Subscribe to data updates."""
        self.subscribers.add(callback)
    
    def unsubscribe(self, callback: Callable[[RealTimeUpdate], None]):
        """Unsubscribe from data updates."""
        self.subscribers.discard(callback)
    
    async def notify_subscribers(self, update: RealTimeUpdate):
        """Notify all subscribers of an update."""
        for callback in self.subscribers:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(update)
                else:
                    callback(update)
            except Exception as e:
                logger.error(f"Error notifying subscriber: {e}")


class OddsDataProvider(RealTimeDataProvider):
    """Real-time odds data provider."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("OddsProvider", config)
        self.session = None
        self.websocket = None
        self.current_odds: Dict[str, Dict] = {}
    
    async def start(self):
        """Start odds data collection."""
        await super().start()
        
        # Create HTTP session
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={'X-RapidAPI-Key': self.config.get('api_key', '')}
        )
        
        # Start polling and WebSocket tasks
        asyncio.create_task(self._poll_odds())
        if self.config.get('websocket_url'):
            asyncio.create_task(self._connect_websocket())
    
    async def stop(self):
        """Stop odds data collection."""
        await super().stop()
        
        if self.websocket:
            await self.websocket.close()
        
        if self.session:
            await self.session.close()
    
    async def _poll_odds(self):
        """Poll odds data at regular intervals."""
        while self.is_active:
            try:
                await self._fetch_odds_update()
                await asyncio.sleep(self.config['update_interval'])
            except Exception as e:
                logger.error(f"Error polling odds: {e}")
                self.error_count += 1
                await asyncio.sleep(self.config['update_interval'])
    
    async def _fetch_odds_update(self):
        """Fetch latest odds from API."""
        try:
            for sport in self.config['sports']:
                url = f"{self.config['base_url']}/sports/{sport}/odds"
                params = {
                    'regions': 'uk,eu',
                    'markets': 'h2h,totals',
                    'oddsFormat': 'decimal',
                    'dateFormat': 'iso'
                }
                
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        await self._process_odds_data(data, sport)
                    else:
                        logger.warning(f"Odds API returned status {response.status}")
                        
        except Exception as e:
            logger.error(f"Failed to fetch odds update: {e}")
            raise
    
    async def _process_odds_data(self, data: List[Dict], sport: str):
        """Process and compare odds data for changes."""
        for match_data in data:
            match_id = match_data.get('id')
            if not match_id:
                continue
            
            # Check for changes
            current_hash = self._hash_odds_data(match_data)
            previous_hash = self.current_odds.get(match_id, {}).get('hash')
            
            if current_hash != previous_hash:
                # Odds have changed, notify subscribers
                update = RealTimeUpdate(
                    source_type=DataSourceType.ODDS,
                    update_type=DataUpdateType.UPDATE if previous_hash else DataUpdateType.CREATE,
                    data={
                        'sport': sport,
                        'match_id': match_id,
                        'home_team': match_data.get('home_team'),
                        'away_team': match_data.get('away_team'),
                        'commence_time': match_data.get('commence_time'),
                        'bookmakers': match_data.get('bookmakers', [])
                    },
                    timestamp=datetime.now(),
                    match_id=match_id
                )
                
                await self.notify_subscribers(update)
                
                # Update cache
                self.current_odds[match_id] = {
                    'hash': current_hash,
                    'data': match_data,
                    'last_updated': datetime.now()
                }
    
    def _hash_odds_data(self, data: Dict) -> str:
        """Create hash of odds data for change detection."""
        import hashlib
        
        # Extract key odds information
        key_data = {
            'home_team': data.get('home_team'),
            'away_team': data.get('away_team'),
            'bookmakers': []
        }
        
        for bookmaker in data.get('bookmakers', []):
            bm_data = {
                'key': bookmaker.get('key'),
                'markets': []
            }
            for market in bookmaker.get('markets', []):
                market_data = {
                    'key': market.get('key'),
                    'outcomes': sorted(market.get('outcomes', []), key=lambda x: x.get('name', ''))
                }
                bm_data['markets'].append(market_data)
            key_data['bookmakers'].append(bm_data)
        
        # Sort bookmakers for consistent hashing
        key_data['bookmakers'].sort(key=lambda x: x['key'])
        
        # Create hash
        data_str = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()
    
    async def _connect_websocket(self):
        """Connect to WebSocket for real-time odds updates."""
        reconnect_count = 0
        
        while self.is_active and reconnect_count < self.config.get('max_reconnects', 5):
            try:
                logger.info("Connecting to odds WebSocket...")
                
                async with websockets.connect(
                    self.config['websocket_url'],
                    extra_headers={'Authorization': f"Bearer {self.config.get('api_key', '')}"}
                ) as websocket:
                    self.websocket = websocket
                    reconnect_count = 0  # Reset on successful connection
                    
                    # Send subscription message
                    subscribe_msg = {
                        'action': 'subscribe',
                        'sports': self.config['sports']
                    }
                    await websocket.send(json.dumps(subscribe_msg))
                    
                    # Listen for messages
                    async for message in websocket:
                        if not self.is_active:
                            break
                        
                        try:
                            data = json.loads(message)
                            await self._process_websocket_message(data)
                        except Exception as e:
                            logger.error(f"Error processing WebSocket message: {e}")
                            
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}")
                reconnect_count += 1
                
                if reconnect_count < self.config.get('max_reconnects', 5):
                    wait_time = self.config.get('reconnect_delay', 5) * reconnect_count
                    logger.info(f"Reconnecting in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error("Max WebSocket reconnection attempts reached")
                    break
    
    async def _process_websocket_message(self, data: Dict[str, Any]):
        """Process WebSocket message."""
        if data.get('type') == 'odds_update':
            update = RealTimeUpdate(
                source_type=DataSourceType.ODDS,
                update_type=DataUpdateType.UPDATE,
                data=data.get('data', {}),
                timestamp=datetime.now(),
                match_id=data.get('match_id')
            )
            await self.notify_subscribers(update)


class FixturesDataProvider(RealTimeDataProvider):
    """Real-time fixtures and live scores provider."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("FixturesProvider", config)
        self.session = None
    
    async def start(self):
        """Start fixtures data collection."""
        await super().start()
        
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={'X-Auth-Token': self.config.get('api_key', '')}
        )
        
        # Start polling task
        asyncio.create_task(self._poll_fixtures())
    
    async def stop(self):
        """Stop fixtures data collection."""
        await super().stop()
        
        if self.session:
            await self.session.close()
    
    async def _poll_fixtures(self):
        """Poll fixtures and live scores."""
        while self.is_active:
            try:
                await self._fetch_fixtures_update()
                await asyncio.sleep(self.config['update_interval'])
            except Exception as e:
                logger.error(f"Error polling fixtures: {e}")
                self.error_count += 1
                await asyncio.sleep(self.config['update_interval'])
    
    async def _fetch_fixtures_update(self):
        """Fetch fixtures and live scores."""
        try:
            # Get today's matches
            today = datetime.now().date()
            url = f"{self.config['base_url']}/matches"
            params = {
                'dateFrom': today.isoformat(),
                'dateTo': today.isoformat()
            }
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    await self._process_fixtures_data(data)
                else:
                    logger.warning(f"Fixtures API returned status {response.status}")
                    
        except Exception as e:
            logger.error(f"Failed to fetch fixtures update: {e}")
            raise
    
    async def _process_fixtures_data(self, data: Dict[str, Any]):
        """Process fixtures data for updates."""
        for match in data.get('matches', []):
            update = RealTimeUpdate(
                source_type=DataSourceType.FIXTURES,
                update_type=DataUpdateType.UPDATE,
                data={
                    'id': match.get('id'),
                    'home_team': match.get('homeTeam', {}).get('name'),
                    'away_team': match.get('awayTeam', {}).get('name'),
                    'status': match.get('status'),
                    'score': match.get('score'),
                    'minute': match.get('minute'),
                    'utc_date': match.get('utcDate')
                },
                timestamp=datetime.now(),
                match_id=str(match.get('id'))
            )
            
            await self.notify_subscribers(update)


class RealTimeDataManager:
    """Manages multiple real-time data providers."""
    
    def __init__(self):
        self.providers: Dict[str, RealTimeDataProvider] = {}
        self.subscribers: Set[Callable] = set()
        self.is_running = False
        self.update_queue = asyncio.Queue()
        self.stats = {
            'total_updates': 0,
            'updates_by_source': {},
            'errors': 0,
            'start_time': None
        }
    
    async def initialize(self, config: Dict[str, Any] = None):
        """Initialize real-time data providers."""
        config = config or REALTIME_CONFIG
        
        try:
            # Initialize odds provider
            if config.get('odds_api', {}).get('api_key'):
                odds_provider = OddsDataProvider(config['odds_api'])
                odds_provider.subscribe(self._handle_provider_update)
                self.providers['odds'] = odds_provider
            
            # Initialize fixtures provider
            if config.get('fixtures_api', {}).get('api_key'):
                fixtures_provider = FixturesDataProvider(config['fixtures_api'])
                fixtures_provider.subscribe(self._handle_provider_update)
                self.providers['fixtures'] = fixtures_provider
            
            logger.info(f"Initialized {len(self.providers)} real-time data providers")
            
        except Exception as e:
            logger.error(f"Failed to initialize real-time data providers: {e}")
            raise
    
    async def start(self):
        """Start all data providers."""
        if self.is_running:
            logger.warning("Real-time data manager is already running")
            return
        
        self.is_running = True
        self.stats['start_time'] = datetime.now()
        
        # Start all providers
        start_tasks = []
        for provider in self.providers.values():
            start_tasks.append(provider.start())
        
        if start_tasks:
            await asyncio.gather(*start_tasks, return_exceptions=True)
        
        # Start update processing task
        asyncio.create_task(self._process_update_queue())
        
        logger.info("Real-time data manager started")
    
    async def stop(self):
        """Stop all data providers."""
        if not self.is_running:
            return
        
        self.is_running = False
        
        # Stop all providers
        stop_tasks = []
        for provider in self.providers.values():
            stop_tasks.append(provider.stop())
        
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)
        
        logger.info("Real-time data manager stopped")
    
    def subscribe(self, callback: Callable[[RealTimeUpdate], None]):
        """Subscribe to real-time updates."""
        self.subscribers.add(callback)
    
    def unsubscribe(self, callback: Callable[[RealTimeUpdate], None]):
        """Unsubscribe from real-time updates."""
        self.subscribers.discard(callback)
    
    async def _handle_provider_update(self, update: RealTimeUpdate):
        """Handle update from a provider."""
        await self.update_queue.put(update)
    
    async def _process_update_queue(self):
        """Process updates from the queue."""
        while self.is_running:
            try:
                # Wait for updates with timeout
                update = await asyncio.wait_for(self.update_queue.get(), timeout=1.0)
                
                # Update statistics
                self.stats['total_updates'] += 1
                source_type = update.source_type.value
                self.stats['updates_by_source'][source_type] = (
                    self.stats['updates_by_source'].get(source_type, 0) + 1
                )
                
                # Notify subscribers
                for callback in self.subscribers:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(update)
                        else:
                            callback(update)
                    except Exception as e:
                        logger.error(f"Error notifying subscriber: {e}")
                        self.stats['errors'] += 1
                
            except asyncio.TimeoutError:
                # No updates, continue
                continue
            except Exception as e:
                logger.error(f"Error processing update queue: {e}")
                self.stats['errors'] += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get real-time data manager statistics."""
        uptime = None
        if self.stats['start_time']:
            uptime = (datetime.now() - self.stats['start_time']).total_seconds()
        
        return {
            'is_running': self.is_running,
            'providers_count': len(self.providers),
            'active_providers': sum(1 for p in self.providers.values() if p.is_active),
            'total_updates': self.stats['total_updates'],
            'updates_by_source': self.stats['updates_by_source'],
            'errors': self.stats['errors'],
            'uptime_seconds': uptime,
            'provider_status': {
                name: {
                    'active': provider.is_active,
                    'last_update': provider.last_update.isoformat() if provider.last_update else None,
                    'error_count': provider.error_count
                }
                for name, provider in self.providers.items()
            }
        }


# Global real-time data manager
realtime_manager = RealTimeDataManager()


async def initialize_realtime_data(config: Dict[str, Any] = None):
    """Initialize the global real-time data manager."""
    await realtime_manager.initialize(config)
    return realtime_manager


class RealTimeDataIntegrator:
    """Integrates real-time data with the database and cache systems."""
    
    def __init__(self, db_manager, cache_manager):
        self.db_manager = db_manager
        self.cache_manager = cache_manager
        self.update_handlers = {
            DataSourceType.ODDS: self._handle_odds_update,
            DataSourceType.FIXTURES: self._handle_fixtures_update,
            DataSourceType.LIVE_SCORES: self._handle_live_scores_update
        }
    
    async def start(self):
        """Start real-time data integration."""
        realtime_manager.subscribe(self._handle_realtime_update)
        logger.info("Real-time data integrator started")
    
    async def stop(self):
        """Stop real-time data integration."""
        realtime_manager.unsubscribe(self._handle_realtime_update)
        logger.info("Real-time data integrator stopped")
    
    async def _handle_realtime_update(self, update: RealTimeUpdate):
        """Handle real-time data update."""
        try:
            handler = self.update_handlers.get(update.source_type)
            if handler:
                await handler(update)
            else:
                logger.warning(f"No handler for update type: {update.source_type}")
                
        except Exception as e:
            logger.error(f"Error handling real-time update: {e}")
    
    async def _handle_odds_update(self, update: RealTimeUpdate):
        """Handle odds update."""
        try:
            # Store in database
            # await self.db_manager.store_odds_data(...)
            
            # Update cache
            cache_key = f"odds_{update.match_id}"
            self.cache_manager.set(cache_key, update.data, ttl=300)  # 5 minutes
            
            logger.debug(f"Processed odds update for match {update.match_id}")
            
        except Exception as e:
            logger.error(f"Error handling odds update: {e}")
    
    async def _handle_fixtures_update(self, update: RealTimeUpdate):
        """Handle fixtures update."""
        try:
            # Store in database
            # await self.db_manager.store_match_data(...)
            
            # Update cache
            cache_key = f"fixtures_{update.match_id}"
            self.cache_manager.set(cache_key, update.data, ttl=600)  # 10 minutes
            
            logger.debug(f"Processed fixtures update for match {update.match_id}")
            
        except Exception as e:
            logger.error(f"Error handling fixtures update: {e}")
    
    async def _handle_live_scores_update(self, update: RealTimeUpdate):
        """Handle live scores update."""
        try:
            # Update live scores in database
            # await self.db_manager.update_live_scores(...)
            
            # Update cache with live data
            cache_key = f"live_scores_{update.match_id}"
            self.cache_manager.set(cache_key, update.data, ttl=60)  # 1 minute
            
            logger.debug(f"Processed live scores update for match {update.match_id}")
            
        except Exception as e:
            logger.error(f"Error handling live scores update: {e}")