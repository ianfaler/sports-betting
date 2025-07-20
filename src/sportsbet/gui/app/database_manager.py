"""Database management system for the sports betting application."""

import asyncio
import logging
import sqlite3
import threading
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import pandas as pd
import numpy as np

try:
    import asyncpg
    import psycopg2
    from psycopg2.extras import RealDictCursor
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'sqlite': {
        'path': Path('.data') / 'sportsbet.db',
        'timeout': 30.0,
        'check_same_thread': False
    },
    'postgres': {
        'host': 'localhost',
        'port': 5432,
        'database': 'sportsbet',
        'user': 'sportsbet_user',
        'password': 'sportsbet_pass',
        'min_connections': 5,
        'max_connections': 20
    }
}


class DatabaseSchema:
    """Database schema definitions and migrations."""
    
    TABLES = {
        'leagues': """
            CREATE TABLE IF NOT EXISTS leagues (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL UNIQUE,
                country VARCHAR(100) NOT NULL,
                division INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        
        'teams': """
            CREATE TABLE IF NOT EXISTS teams (
                id SERIAL PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                league_id INTEGER REFERENCES leagues(id),
                country VARCHAR(100),
                founded_year INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(name, league_id)
            )
        """,
        
        'matches': """
            CREATE TABLE IF NOT EXISTS matches (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                league_id INTEGER REFERENCES leagues(id),
                home_team_id INTEGER REFERENCES teams(id),
                away_team_id INTEGER REFERENCES teams(id),
                home_goals INTEGER,
                away_goals INTEGER,
                status VARCHAR(20) DEFAULT 'scheduled',
                season VARCHAR(10),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, home_team_id, away_team_id)
            )
        """,
        
        'match_statistics': """
            CREATE TABLE IF NOT EXISTS match_statistics (
                id SERIAL PRIMARY KEY,
                match_id INTEGER REFERENCES matches(id),
                team_id INTEGER REFERENCES teams(id),
                shots INTEGER DEFAULT 0,
                shots_on_target INTEGER DEFAULT 0,
                corners INTEGER DEFAULT 0,
                fouls INTEGER DEFAULT 0,
                yellow_cards INTEGER DEFAULT 0,
                red_cards INTEGER DEFAULT 0,
                possession_percentage DECIMAL(5,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(match_id, team_id)
            )
        """,
        
        'odds': """
            CREATE TABLE IF NOT EXISTS odds (
                id SERIAL PRIMARY KEY,
                match_id INTEGER REFERENCES matches(id),
                market_type VARCHAR(50) NOT NULL,
                outcome VARCHAR(50) NOT NULL,
                odds_value DECIMAL(10,3) NOT NULL,
                odds_type VARCHAR(50) DEFAULT 'market_average',
                bookmaker VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        
        'predictions': """
            CREATE TABLE IF NOT EXISTS predictions (
                id SERIAL PRIMARY KEY,
                match_id INTEGER REFERENCES matches(id),
                model_name VARCHAR(100) NOT NULL,
                market_type VARCHAR(50) NOT NULL,
                outcome VARCHAR(50) NOT NULL,
                probability DECIMAL(5,4) NOT NULL,
                confidence_score DECIMAL(5,4),
                expected_value DECIMAL(10,4),
                is_value_bet BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        
        'bet_history': """
            CREATE TABLE IF NOT EXISTS bet_history (
                id SERIAL PRIMARY KEY,
                match_id INTEGER REFERENCES matches(id),
                prediction_id INTEGER REFERENCES predictions(id),
                stake DECIMAL(10,2) NOT NULL,
                odds DECIMAL(10,3) NOT NULL,
                potential_return DECIMAL(10,2) NOT NULL,
                actual_return DECIMAL(10,2) DEFAULT 0,
                status VARCHAR(20) DEFAULT 'pending',
                placed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                settled_at TIMESTAMP
            )
        """,
        
        'data_sources': """
            CREATE TABLE IF NOT EXISTS data_sources (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL UNIQUE,
                url VARCHAR(500) NOT NULL,
                source_type VARCHAR(50) NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                last_updated TIMESTAMP,
                error_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        
        'data_cache': """
            CREATE TABLE IF NOT EXISTS data_cache (
                id SERIAL PRIMARY KEY,
                cache_key VARCHAR(255) NOT NULL UNIQUE,
                data_type VARCHAR(50) NOT NULL,
                data_content BYTEA NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                access_count INTEGER DEFAULT 0,
                last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
    }
    
    INDEXES = [
        "CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(date)",
        "CREATE INDEX IF NOT EXISTS idx_matches_league ON matches(league_id)",
        "CREATE INDEX IF NOT EXISTS idx_matches_teams ON matches(home_team_id, away_team_id)",
        "CREATE INDEX IF NOT EXISTS idx_odds_match ON odds(match_id)",
        "CREATE INDEX IF NOT EXISTS idx_odds_market ON odds(market_type, outcome)",
        "CREATE INDEX IF NOT EXISTS idx_predictions_match ON predictions(match_id)",
        "CREATE INDEX IF NOT EXISTS idx_predictions_model ON predictions(model_name)",
        "CREATE INDEX IF NOT EXISTS idx_bet_history_match ON bet_history(match_id)",
        "CREATE INDEX IF NOT EXISTS idx_data_cache_expires ON data_cache(expires_at)",
        "CREATE INDEX IF NOT EXISTS idx_data_cache_type ON data_cache(data_type)"
    ]


class DatabaseManager:
    """Main database manager supporting both SQLite and PostgreSQL."""
    
    def __init__(self, db_type: str = 'sqlite', config: Dict[str, Any] = None):
        self.db_type = db_type.lower()
        self.config = config or DB_CONFIG.get(self.db_type, {})
        self.pool = None
        self._connection = None
        self._lock = threading.RLock()
        
        if self.db_type == 'postgres' and not POSTGRES_AVAILABLE:
            logger.warning("PostgreSQL dependencies not available, falling back to SQLite")
            self.db_type = 'sqlite'
            self.config = DB_CONFIG['sqlite']
        
        # Ensure data directory exists for SQLite
        if self.db_type == 'sqlite':
            self.config['path'].parent.mkdir(exist_ok=True)
    
    async def initialize(self):
        """Initialize database connection and schema."""
        try:
            if self.db_type == 'postgres':
                await self._init_postgres()
            else:
                await self._init_sqlite()
            
            await self._create_schema()
            await self._populate_initial_data()
            
            logger.info(f"Database initialized successfully ({self.db_type})")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    async def _init_postgres(self):
        """Initialize PostgreSQL connection pool."""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                min_size=self.config['min_connections'],
                max_size=self.config['max_connections'],
                command_timeout=30
            )
            logger.info("PostgreSQL connection pool created")
        except Exception as e:
            logger.error(f"Failed to create PostgreSQL pool: {e}")
            raise
    
    async def _init_sqlite(self):
        """Initialize SQLite connection."""
        try:
            # SQLite doesn't have async connection pools, so we'll use a simple connection
            self._connection = sqlite3.connect(
                self.config['path'],
                timeout=self.config['timeout'],
                check_same_thread=self.config['check_same_thread']
            )
            self._connection.row_factory = sqlite3.Row
            logger.info(f"SQLite database connected: {self.config['path']}")
        except Exception as e:
            logger.error(f"Failed to connect to SQLite: {e}")
            raise
    
    async def _create_schema(self):
        """Create database schema."""
        try:
            async with self.get_connection() as conn:
                # Create tables
                for table_name, table_sql in DatabaseSchema.TABLES.items():
                    if self.db_type == 'sqlite':
                        # Convert PostgreSQL syntax to SQLite
                        table_sql = table_sql.replace('SERIAL PRIMARY KEY', 'INTEGER PRIMARY KEY AUTOINCREMENT')
                        table_sql = table_sql.replace('DECIMAL(10,3)', 'REAL')
                        table_sql = table_sql.replace('DECIMAL(5,2)', 'REAL')
                        table_sql = table_sql.replace('DECIMAL(5,4)', 'REAL')
                        table_sql = table_sql.replace('DECIMAL(10,2)', 'REAL')
                        table_sql = table_sql.replace('DECIMAL(10,4)', 'REAL')
                        table_sql = table_sql.replace('TIMESTAMP DEFAULT CURRENT_TIMESTAMP', 'DATETIME DEFAULT CURRENT_TIMESTAMP')
                        table_sql = table_sql.replace('BYTEA', 'BLOB')
                    
                    if self.db_type == 'postgres':
                        await conn.execute(table_sql)
                    else:
                        conn.execute(table_sql)
                
                # Create indexes
                for index_sql in DatabaseSchema.INDEXES:
                    if self.db_type == 'postgres':
                        await conn.execute(index_sql)
                    else:
                        conn.execute(index_sql)
                
                if self.db_type == 'sqlite':
                    conn.commit()
                
                logger.info("Database schema created successfully")
                
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            raise
    
    async def _populate_initial_data(self):
        """Populate database with initial reference data."""
        try:
            # Insert common leagues
            leagues_data = [
                ('Premier League', 'England', 1),
                ('La Liga', 'Spain', 1),
                ('Serie A', 'Italy', 1),
                ('Bundesliga', 'Germany', 1),
                ('Ligue 1', 'France', 1),
                ('Championship', 'England', 2),
                ('La Liga 2', 'Spain', 2),
                ('Serie B', 'Italy', 2),
                ('2. Bundesliga', 'Germany', 2),
                ('Ligue 2', 'France', 2)
            ]
            
            async with self.get_connection() as conn:
                for name, country, division in leagues_data:
                    if self.db_type == 'postgres':
                        await conn.execute("""
                            INSERT INTO leagues (name, country, division) 
                            VALUES ($1, $2, $3) 
                            ON CONFLICT (name) DO NOTHING
                        """, name, country, division)
                    else:
                        conn.execute("""
                            INSERT OR IGNORE INTO leagues (name, country, division) 
                            VALUES (?, ?, ?)
                        """, (name, country, division))
                
                if self.db_type == 'sqlite':
                    conn.commit()
            
            logger.info("Initial data populated successfully")
            
        except Exception as e:
            logger.error(f"Failed to populate initial data: {e}")
            # Don't raise here as this is not critical
    
    @asynccontextmanager
    async def get_connection(self):
        """Get database connection context manager."""
        if self.db_type == 'postgres':
            async with self.pool.acquire() as conn:
                yield conn
        else:
            with self._lock:
                yield self._connection
    
    async def close(self):
        """Close database connections."""
        try:
            if self.db_type == 'postgres' and self.pool:
                await self.pool.close()
                logger.info("PostgreSQL connection pool closed")
            elif self.db_type == 'sqlite' and self._connection:
                self._connection.close()
                logger.info("SQLite connection closed")
        except Exception as e:
            logger.error(f"Error closing database connections: {e}")
    
    async def execute_query(self, query: str, params: Tuple = None) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return results."""
        try:
            async with self.get_connection() as conn:
                if self.db_type == 'postgres':
                    if params:
                        rows = await conn.fetch(query, *params)
                    else:
                        rows = await conn.fetch(query)
                    return [dict(row) for row in rows]
                else:
                    if params:
                        cursor = conn.execute(query, params)
                    else:
                        cursor = conn.execute(query)
                    rows = cursor.fetchall()
                    return [dict(row) for row in rows]
                    
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    async def execute_command(self, command: str, params: Tuple = None) -> int:
        """Execute an INSERT/UPDATE/DELETE command."""
        try:
            async with self.get_connection() as conn:
                if self.db_type == 'postgres':
                    if params:
                        result = await conn.execute(command, *params)
                    else:
                        result = await conn.execute(command)
                    # PostgreSQL returns status string, extract affected rows
                    return int(result.split()[-1]) if result else 0
                else:
                    if params:
                        cursor = conn.execute(command, params)
                    else:
                        cursor = conn.execute(command)
                    conn.commit()
                    return cursor.rowcount
                    
        except Exception as e:
            logger.error(f"Command execution failed: {e}")
            raise
    
    async def store_match_data(self, match_data: pd.DataFrame) -> int:
        """Store match data in the database."""
        try:
            stored_count = 0
            
            async with self.get_connection() as conn:
                for _, row in match_data.iterrows():
                    try:
                        # Store match
                        if self.db_type == 'postgres':
                            match_id = await conn.fetchval("""
                                INSERT INTO matches (date, league_id, home_team_id, away_team_id, 
                                                   home_goals, away_goals, status, season)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                                ON CONFLICT (date, home_team_id, away_team_id) 
                                DO UPDATE SET home_goals = EXCLUDED.home_goals,
                                            away_goals = EXCLUDED.away_goals,
                                            status = EXCLUDED.status
                                RETURNING id
                            """, row['date'], row['league_id'], row['home_team_id'], 
                                row['away_team_id'], row['home_goals'], row['away_goals'],
                                row.get('status', 'completed'), row.get('season', '2024'))
                        else:
                            conn.execute("""
                                INSERT OR REPLACE INTO matches 
                                (date, league_id, home_team_id, away_team_id, home_goals, away_goals, status, season)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """, (row['date'], row['league_id'], row['home_team_id'], 
                                 row['away_team_id'], row['home_goals'], row['away_goals'],
                                 row.get('status', 'completed'), row.get('season', '2024')))
                            match_id = conn.lastrowid
                        
                        stored_count += 1
                        
                    except Exception as e:
                        logger.warning(f"Failed to store match data for row {row.name}: {e}")
                        continue
                
                if self.db_type == 'sqlite':
                    conn.commit()
            
            logger.info(f"Stored {stored_count} matches in database")
            return stored_count
            
        except Exception as e:
            logger.error(f"Failed to store match data: {e}")
            raise
    
    async def store_odds_data(self, odds_data: pd.DataFrame) -> int:
        """Store odds data in the database."""
        try:
            stored_count = 0
            
            async with self.get_connection() as conn:
                for _, row in odds_data.iterrows():
                    try:
                        if self.db_type == 'postgres':
                            await conn.execute("""
                                INSERT INTO odds (match_id, market_type, outcome, odds_value, 
                                                odds_type, bookmaker)
                                VALUES ($1, $2, $3, $4, $5, $6)
                                ON CONFLICT (match_id, market_type, outcome, odds_type, bookmaker)
                                DO UPDATE SET odds_value = EXCLUDED.odds_value,
                                            updated_at = CURRENT_TIMESTAMP
                            """, row['match_id'], row['market_type'], row['outcome'],
                                row['odds_value'], row.get('odds_type', 'market_average'),
                                row.get('bookmaker', 'average'))
                        else:
                            conn.execute("""
                                INSERT OR REPLACE INTO odds 
                                (match_id, market_type, outcome, odds_value, odds_type, bookmaker)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (row['match_id'], row['market_type'], row['outcome'],
                                 row['odds_value'], row.get('odds_type', 'market_average'),
                                 row.get('bookmaker', 'average')))
                        
                        stored_count += 1
                        
                    except Exception as e:
                        logger.warning(f"Failed to store odds data for row {row.name}: {e}")
                        continue
                
                if self.db_type == 'sqlite':
                    conn.commit()
            
            logger.info(f"Stored {stored_count} odds entries in database")
            return stored_count
            
        except Exception as e:
            logger.error(f"Failed to store odds data: {e}")
            raise
    
    async def get_match_data(
        self, 
        league_ids: List[int] = None,
        date_from: datetime = None,
        date_to: datetime = None,
        limit: int = None
    ) -> pd.DataFrame:
        """Retrieve match data from database."""
        try:
            query_parts = ["SELECT * FROM matches WHERE 1=1"]
            params = []
            
            if league_ids:
                placeholders = ','.join(['?' if self.db_type == 'sqlite' else f'${len(params)+i+1}' 
                                       for i in range(len(league_ids))])
                query_parts.append(f"AND league_id IN ({placeholders})")
                params.extend(league_ids)
            
            if date_from:
                placeholder = '?' if self.db_type == 'sqlite' else f'${len(params)+1}'
                query_parts.append(f"AND date >= {placeholder}")
                params.append(date_from)
            
            if date_to:
                placeholder = '?' if self.db_type == 'sqlite' else f'${len(params)+1}'
                query_parts.append(f"AND date <= {placeholder}")
                params.append(date_to)
            
            query_parts.append("ORDER BY date DESC")
            
            if limit:
                query_parts.append(f"LIMIT {limit}")
            
            query = ' '.join(query_parts)
            rows = await self.execute_query(query, tuple(params) if params else None)
            
            return pd.DataFrame(rows)
            
        except Exception as e:
            logger.error(f"Failed to retrieve match data: {e}")
            raise
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform database health check."""
        try:
            start_time = datetime.now()
            
            # Test basic connectivity
            test_query = "SELECT 1 as test" if self.db_type == 'postgres' else "SELECT 1 as test"
            result = await self.execute_query(test_query)
            
            response_time = (datetime.now() - start_time).total_seconds()
            
            # Get table counts
            tables_info = {}
            for table_name in DatabaseSchema.TABLES.keys():
                try:
                    count_result = await self.execute_query(f"SELECT COUNT(*) as count FROM {table_name}")
                    tables_info[table_name] = count_result[0]['count']
                except Exception as e:
                    tables_info[table_name] = f"Error: {e}"
            
            return {
                'status': 'healthy',
                'database_type': self.db_type,
                'response_time_seconds': response_time,
                'tables': tables_info,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }


# Global database manager instance
db_manager = DatabaseManager()


async def initialize_database(db_type: str = 'sqlite', config: Dict[str, Any] = None):
    """Initialize the global database manager."""
    global db_manager
    db_manager = DatabaseManager(db_type, config)
    await db_manager.initialize()
    return db_manager


async def migrate_csv_to_database(csv_data: pd.DataFrame, data_type: str = 'matches') -> int:
    """Migrate CSV data to database format."""
    try:
        if data_type == 'matches':
            return await db_manager.store_match_data(csv_data)
        elif data_type == 'odds':
            return await db_manager.store_odds_data(csv_data)
        else:
            raise ValueError(f"Unsupported data type: {data_type}")
            
    except Exception as e:
        logger.error(f"Failed to migrate CSV data: {e}")
        raise