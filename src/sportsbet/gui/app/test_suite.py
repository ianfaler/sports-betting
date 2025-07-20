"""Comprehensive testing suite for the sports betting application."""

import asyncio
import json
import logging
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest

from .cache_manager import CacheManager
from .database_manager import DatabaseManager, DatabaseSchema
from .error_handler import ErrorHandler, ErrorCategory, ErrorSeverity
from .memory_manager import MemoryManager
from .realtime_data import RealTimeDataManager, DataSourceType, DataUpdateType
from .secure_uploads import validate_file_upload, safe_deserialize_dataloader

logger = logging.getLogger(__name__)


class TestDataGenerator:
    """Generates test data for various testing scenarios."""
    
    @staticmethod
    def generate_match_data(num_matches: int = 10) -> pd.DataFrame:
        """Generate realistic match data for testing."""
        import random
        from datetime import datetime, timedelta
        
        leagues = ['Premier League', 'La Liga', 'Serie A', 'Bundesliga', 'Ligue 1']
        teams = {
            'Premier League': ['Arsenal', 'Chelsea', 'Liverpool', 'Manchester City', 'Manchester United', 'Tottenham'],
            'La Liga': ['Barcelona', 'Real Madrid', 'Atletico Madrid', 'Sevilla', 'Valencia', 'Villarreal'],
            'Serie A': ['Juventus', 'AC Milan', 'Inter Milan', 'AS Roma', 'Napoli', 'Lazio'],
            'Bundesliga': ['Bayern Munich', 'Borussia Dortmund', 'RB Leipzig', 'Bayer Leverkusen', 'Wolfsburg', 'Frankfurt'],
            'Ligue 1': ['PSG', 'Lyon', 'Marseille', 'Monaco', 'Lille', 'Nice']
        }
        
        matches = []
        for i in range(num_matches):
            league = random.choice(leagues)
            league_teams = teams[league]
            home_team = random.choice(league_teams)
            away_team = random.choice([t for t in league_teams if t != home_team])
            
            # Generate realistic scores
            home_goals = random.choices([0, 1, 2, 3, 4], weights=[15, 30, 35, 15, 5])[0]
            away_goals = random.choices([0, 1, 2, 3, 4], weights=[20, 35, 30, 12, 3])[0]
            
            match = {
                'id': i + 1,
                'date': (datetime.now() - timedelta(days=random.randint(1, 365))).date(),
                'league': league,
                'league_id': leagues.index(league) + 1,
                'home_team': home_team,
                'home_team_id': league_teams.index(home_team) + 1,
                'away_team': away_team,
                'away_team_id': league_teams.index(away_team) + 1,
                'home_goals': home_goals,
                'away_goals': away_goals,
                'status': 'completed',
                'season': '2024'
            }
            matches.append(match)
        
        return pd.DataFrame(matches)
    
    @staticmethod
    def generate_odds_data(num_odds: int = 50) -> pd.DataFrame:
        """Generate realistic odds data for testing."""
        import random
        
        market_types = ['match_winner', 'total_goals', 'both_teams_score']
        outcomes = {
            'match_winner': ['home', 'away', 'draw'],
            'total_goals': ['over_2.5', 'under_2.5'],
            'both_teams_score': ['yes', 'no']
        }
        
        odds_data = []
        for i in range(num_odds):
            match_id = random.randint(1, 10)
            market_type = random.choice(market_types)
            outcome = random.choice(outcomes[market_type])
            
            # Generate realistic odds
            if outcome == 'home':
                odds_value = round(random.uniform(1.5, 4.0), 2)
            elif outcome == 'away':
                odds_value = round(random.uniform(1.8, 5.0), 2)
            elif outcome == 'draw':
                odds_value = round(random.uniform(2.8, 4.5), 2)
            else:
                odds_value = round(random.uniform(1.4, 3.0), 2)
            
            odds = {
                'id': i + 1,
                'match_id': match_id,
                'market_type': market_type,
                'outcome': outcome,
                'odds_value': odds_value,
                'odds_type': 'market_average',
                'bookmaker': random.choice(['Bet365', 'William Hill', 'Paddy Power', 'Betfair'])
            }
            odds_data.append(odds)
        
        return pd.DataFrame(odds_data)
    
    @staticmethod
    def generate_test_file(file_type: str = 'dataloader', size_mb: float = 1.0) -> bytes:
        """Generate test file content for upload testing."""
        import pickle
        
        if file_type == 'dataloader':
            # Create a mock dataloader object
            class MockDataLoader:
                def extract_train_data(self, *args, **kwargs):
                    return TestDataGenerator.generate_match_data(), None, None
                
                def extract_fixtures_data(self, *args, **kwargs):
                    return TestDataGenerator.generate_match_data(5), None, None
                
                def get_odds_types(self):
                    return ['market_average', 'market_maximum']
            
            mock_data = MockDataLoader()
            content = pickle.dumps(mock_data)
            
            # Pad to desired size
            target_size = int(size_mb * 1024 * 1024)
            if len(content) < target_size:
                padding = b'0' * (target_size - len(content))
                content += padding
            
            return content
        
        elif file_type == 'invalid':
            # Return non-pickle content
            return b'This is not a valid pickle file' * 1000
        
        else:
            raise ValueError(f"Unknown file type: {file_type}")


class TestMemoryManager(unittest.TestCase):
    """Test cases for memory management system."""
    
    def setUp(self):
        """Set up test environment."""
        self.memory_manager = MemoryManager(max_memory=10 * 1024 * 1024)  # 10MB for testing
    
    def tearDown(self):
        """Clean up test environment."""
        self.memory_manager.cleanup_all()
        self.memory_manager.stop_monitoring()
    
    def test_memory_tracking(self):
        """Test memory object tracking."""
        test_data = "x" * 1000  # 1KB string
        
        # Track object
        result = self.memory_manager.track_object("test_data", test_data)
        self.assertTrue(result)
        
        # Check if object is tracked
        stats = self.memory_manager.get_memory_stats()
        self.assertEqual(stats['tracked_objects'], 1)
        self.assertGreater(stats['tracked_size'], 0)
        
        # Untrack object
        self.memory_manager.untrack_object("test_data")
        stats = self.memory_manager.get_memory_stats()
        self.assertEqual(stats['tracked_objects'], 0)
    
    def test_memory_limit_enforcement(self):
        """Test memory limit enforcement."""
        # Create large data that exceeds limit
        large_data = "x" * (15 * 1024 * 1024)  # 15MB (exceeds 10MB limit)
        
        # Should fail to track due to size limit
        result = self.memory_manager.track_object("large_data", large_data)
        self.assertFalse(result)
    
    def test_memory_cleanup(self):
        """Test memory cleanup functionality."""
        # Add multiple objects
        for i in range(5):
            data = f"test_data_{i}" * 100
            self.memory_manager.track_object(f"data_{i}", data)
        
        initial_count = len(self.memory_manager.tracked_objects)
        self.assertEqual(initial_count, 5)
        
        # Trigger cleanup
        self.memory_manager.gentle_cleanup()
        
        # Should have fewer objects
        final_count = len(self.memory_manager.tracked_objects)
        self.assertLess(final_count, initial_count)


class TestCacheManager(unittest.IsolatedAsyncioTestCase):
    """Test cases for cache management system."""
    
    async def asyncSetUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.cache_manager = CacheManager(
            cache_dir=Path(self.temp_dir),
            max_size=1024 * 1024  # 1MB for testing
        )
        self.cache_manager.start_monitoring()
    
    async def asyncTearDown(self):
        """Clean up test environment."""
        self.cache_manager.stop_monitoring()
        self.cache_manager.clear_all()
    
    async def test_cache_operations(self):
        """Test basic cache operations."""
        key = "test_key"
        data = {"test": "data", "number": 42}
        
        # Set data
        result = self.cache_manager.set(key, data, ttl=60)
        self.assertTrue(result)
        
        # Get data
        retrieved_data = self.cache_manager.get(key)
        self.assertEqual(retrieved_data, data)
        
        # Delete data
        deleted = self.cache_manager.delete(key)
        self.assertTrue(deleted)
        
        # Should not exist now
        retrieved_data = self.cache_manager.get(key)
        self.assertIsNone(retrieved_data)
    
    async def test_cache_expiration(self):
        """Test cache TTL and expiration."""
        key = "expiring_key"
        data = "expiring_data"
        
        # Set with very short TTL
        self.cache_manager.set(key, data, ttl=1)
        
        # Should exist immediately
        retrieved = self.cache_manager.get(key)
        self.assertEqual(retrieved, data)
        
        # Wait for expiration
        await asyncio.sleep(2)
        
        # Should be expired
        retrieved = self.cache_manager.get(key)
        self.assertIsNone(retrieved)
    
    async def test_cache_size_limit(self):
        """Test cache size limit enforcement."""
        # Fill cache near limit
        large_data = "x" * 100000  # 100KB
        
        for i in range(12):  # Should exceed 1MB limit
            self.cache_manager.set(f"large_data_{i}", large_data)
        
        # Wait for cleanup
        await asyncio.sleep(2)
        
        # Check that some items were evicted
        stats = self.cache_manager.get_stats()
        self.assertLess(stats['total_size'], self.cache_manager.max_size)


class TestDatabaseManager(unittest.IsolatedAsyncioTestCase):
    """Test cases for database management system."""
    
    async def asyncSetUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        config = {
            'path': Path(self.temp_dir) / 'test.db',
            'timeout': 30.0,
            'check_same_thread': False
        }
        self.db_manager = DatabaseManager('sqlite', config)
        await self.db_manager.initialize()
    
    async def asyncTearDown(self):
        """Clean up test environment."""
        await self.db_manager.close()
    
    async def test_database_schema_creation(self):
        """Test database schema creation."""
        # Check that tables exist
        for table_name in DatabaseSchema.TABLES.keys():
            query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
            result = await self.db_manager.execute_query(query)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]['name'], table_name)
    
    async def test_match_data_storage(self):
        """Test storing match data."""
        match_data = TestDataGenerator.generate_match_data(5)
        
        # Store data
        stored_count = await self.db_manager.store_match_data(match_data)
        self.assertEqual(stored_count, 5)
        
        # Retrieve data
        retrieved_data = await self.db_manager.get_match_data(limit=10)
        self.assertEqual(len(retrieved_data), 5)
    
    async def test_database_health_check(self):
        """Test database health check."""
        health = await self.db_manager.health_check()
        
        self.assertEqual(health['status'], 'healthy')
        self.assertEqual(health['database_type'], 'sqlite')
        self.assertGreater(health['response_time_seconds'], 0)
        self.assertIsInstance(health['tables'], dict)


class TestSecureUploads(unittest.TestCase):
    """Test cases for secure file upload system."""
    
    def test_valid_file_validation(self):
        """Test validation of valid files."""
        valid_content = TestDataGenerator.generate_test_file('dataloader', 0.1)
        
        result = validate_file_upload('test_dataloader.pkl', valid_content)
        
        self.assertTrue(result['valid'])
        self.assertIsNone(result['error'])
        self.assertIsNotNone(result['security_hash'])
    
    def test_invalid_file_extension(self):
        """Test validation of invalid file extensions."""
        content = b"test content"
        
        result = validate_file_upload('test_file.txt', content)
        
        self.assertFalse(result['valid'])
        self.assertIn('not allowed', result['error'])
    
    def test_file_size_limit(self):
        """Test file size limit enforcement."""
        large_content = TestDataGenerator.generate_test_file('dataloader', 60)  # 60MB
        
        result = validate_file_upload('large_file.pkl', large_content)
        
        self.assertFalse(result['valid'])
        self.assertIn('exceeds limit', result['error'])
    
    def test_empty_file(self):
        """Test validation of empty files."""
        empty_content = b""
        
        result = validate_file_upload('empty_file.pkl', empty_content)
        
        self.assertFalse(result['valid'])
        self.assertIn('empty', result['error'])
    
    def test_safe_deserialization(self):
        """Test safe deserialization of valid data."""
        valid_content = TestDataGenerator.generate_test_file('dataloader', 0.1)
        
        dataloader, error = safe_deserialize_dataloader(valid_content, 'test.pkl')
        
        self.assertIsNotNone(dataloader)
        self.assertIsNone(error)
        # Check required methods exist
        self.assertTrue(hasattr(dataloader, 'extract_train_data'))
        self.assertTrue(hasattr(dataloader, 'extract_fixtures_data'))
        self.assertTrue(hasattr(dataloader, 'get_odds_types'))
    
    def test_invalid_deserialization(self):
        """Test safe deserialization of invalid data."""
        invalid_content = TestDataGenerator.generate_test_file('invalid')
        
        dataloader, error = safe_deserialize_dataloader(invalid_content, 'test.pkl')
        
        self.assertIsNone(dataloader)
        self.assertIsNotNone(error)


class TestErrorHandler(unittest.TestCase):
    """Test cases for error handling system."""
    
    def setUp(self):
        """Set up test environment."""
        self.error_handler = ErrorHandler()
    
    def test_network_error_handling(self):
        """Test handling of network errors."""
        network_error = Exception("Connection timeout")
        
        user_error = self.error_handler.handle_exception(network_error, "network test")
        
        self.assertEqual(user_error.category.value, 'network')
        self.assertIn('timeout', user_error.title.lower())
        self.assertGreater(len(user_error.recovery_suggestions), 0)
    
    def test_memory_error_handling(self):
        """Test handling of memory errors."""
        memory_error = MemoryError("Out of memory")
        
        user_error = self.error_handler.handle_exception(memory_error, "memory test")
        
        self.assertEqual(user_error.category.value, 'memory')
        self.assertEqual(user_error.severity, ErrorSeverity.CRITICAL)
        self.assertIn('memory', user_error.title.lower())
    
    def test_file_upload_error_handling(self):
        """Test handling of file upload errors."""
        file_error = Exception("File size exceeds limit")
        
        user_error = self.error_handler.handle_exception(file_error, "file upload")
        
        self.assertEqual(user_error.category.value, 'file_upload')
        self.assertIn('size', user_error.title.lower())
    
    def test_error_logging(self):
        """Test error logging functionality."""
        with patch('logging.getLogger') as mock_logger:
            mock_log = Mock()
            mock_logger.return_value = mock_log
            
            test_error = Exception("Test error")
            self.error_handler.handle_exception(test_error)
            
            # Should have logged the error
            self.assertTrue(mock_log.error.called)


class TestRealTimeData(unittest.IsolatedAsyncioTestCase):
    """Test cases for real-time data system."""
    
    async def asyncSetUp(self):
        """Set up test environment."""
        self.realtime_manager = RealTimeDataManager()
    
    async def asyncTearDown(self):
        """Clean up test environment."""
        await self.realtime_manager.stop()
    
    async def test_realtime_manager_initialization(self):
        """Test real-time manager initialization."""
        # Mock config without API keys
        config = {
            'odds_api': {'api_key': None},
            'fixtures_api': {'api_key': None}
        }
        
        await self.realtime_manager.initialize(config)
        
        # Should initialize without providers due to missing API keys
        self.assertEqual(len(self.realtime_manager.providers), 0)
    
    async def test_subscriber_management(self):
        """Test subscriber management."""
        callback_called = False
        
        def test_callback(update):
            nonlocal callback_called
            callback_called = True
        
        # Subscribe
        self.realtime_manager.subscribe(test_callback)
        self.assertIn(test_callback, self.realtime_manager.subscribers)
        
        # Unsubscribe
        self.realtime_manager.unsubscribe(test_callback)
        self.assertNotIn(test_callback, self.realtime_manager.subscribers)
    
    async def test_stats_collection(self):
        """Test statistics collection."""
        stats = self.realtime_manager.get_stats()
        
        self.assertIn('is_running', stats)
        self.assertIn('providers_count', stats)
        self.assertIn('total_updates', stats)
        self.assertIn('errors', stats)


class IntegrationTestSuite:
    """Integration tests for the complete system."""
    
    def __init__(self):
        self.temp_dir = None
        self.db_manager = None
        self.cache_manager = None
        self.memory_manager = None
    
    async def setup(self):
        """Set up integration test environment."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Initialize database
        db_config = {
            'path': Path(self.temp_dir) / 'integration_test.db',
            'timeout': 30.0,
            'check_same_thread': False
        }
        self.db_manager = DatabaseManager('sqlite', db_config)
        await self.db_manager.initialize()
        
        # Initialize cache
        self.cache_manager = CacheManager(
            cache_dir=Path(self.temp_dir) / 'cache',
            max_size=10 * 1024 * 1024
        )
        self.cache_manager.start_monitoring()
        
        # Initialize memory manager
        self.memory_manager = MemoryManager(max_memory=50 * 1024 * 1024)
        self.memory_manager.start_monitoring()
    
    async def teardown(self):
        """Clean up integration test environment."""
        if self.db_manager:
            await self.db_manager.close()
        
        if self.cache_manager:
            self.cache_manager.stop_monitoring()
            self.cache_manager.clear_all()
        
        if self.memory_manager:
            self.memory_manager.stop_monitoring()
            self.memory_manager.cleanup_all()
    
    async def test_data_flow_integration(self):
        """Test complete data flow from upload to storage."""
        # Generate test data
        match_data = TestDataGenerator.generate_match_data(10)
        odds_data = TestDataGenerator.generate_odds_data(20)
        
        # Store in database
        matches_stored = await self.db_manager.store_match_data(match_data)
        odds_stored = await self.db_manager.store_odds_data(odds_data)
        
        assert matches_stored == 10
        assert odds_stored == 20
        
        # Cache the data
        cache_key = "test_matches"
        self.cache_manager.set(cache_key, match_data.to_dict('records'))
        
        # Retrieve from cache
        cached_data = self.cache_manager.get(cache_key)
        assert cached_data is not None
        assert len(cached_data) == 10
        
        # Track in memory manager
        result = self.memory_manager.track_object("match_data", match_data)
        assert result is True
        
        # Verify memory stats
        stats = self.memory_manager.get_memory_stats()
        assert stats['tracked_objects'] == 1
    
    async def test_error_recovery_integration(self):
        """Test error handling and recovery across systems."""
        error_handler = ErrorHandler()
        
        # Simulate database error
        try:
            await self.db_manager.execute_query("INVALID SQL QUERY")
        except Exception as e:
            user_error = error_handler.handle_exception(e, "database test")
            assert user_error.category == ErrorCategory.UNKNOWN
            assert len(user_error.recovery_suggestions) > 0
        
        # Test system recovery
        health = await self.db_manager.health_check()
        assert health['status'] == 'healthy'


async def run_integration_tests():
    """Run all integration tests."""
    integration_suite = IntegrationTestSuite()
    
    try:
        await integration_suite.setup()
        
        print("Running integration tests...")
        
        # Run data flow test
        await integration_suite.test_data_flow_integration()
        print("✅ Data flow integration test passed")
        
        # Run error recovery test
        await integration_suite.test_error_recovery_integration()
        print("✅ Error recovery integration test passed")
        
        print("🎉 All integration tests passed!")
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        raise
    
    finally:
        await integration_suite.teardown()


def run_unit_tests():
    """Run all unit tests."""
    print("Running unit tests...")
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestMemoryManager,
        TestSecureUploads,
        TestErrorHandler
    ]
    
    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    if result.wasSuccessful():
        print("🎉 All unit tests passed!")
    else:
        print(f"❌ {len(result.failures)} test(s) failed, {len(result.errors)} error(s)")
        return False
    
    return True


async def run_async_unit_tests():
    """Run async unit tests."""
    print("Running async unit tests...")
    
    test_classes = [
        TestCacheManager,
        TestDatabaseManager,
        TestRealTimeData
    ]
    
    for test_class in test_classes:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        
        if not result.wasSuccessful():
            print(f"❌ Async tests failed for {test_class.__name__}")
            return False
    
    print("🎉 All async unit tests passed!")
    return True


async def run_all_tests():
    """Run complete test suite."""
    print("🧪 Starting comprehensive test suite...")
    
    try:
        # Run unit tests
        unit_success = run_unit_tests()
        if not unit_success:
            return False
        
        # Run async unit tests
        async_success = await run_async_unit_tests()
        if not async_success:
            return False
        
        # Run integration tests
        await run_integration_tests()
        
        print("🎉 ALL TESTS PASSED! System is ready for production.")
        return True
        
    except Exception as e:
        print(f"❌ Test suite failed: {e}")
        return False


if __name__ == "__main__":
    # Run the complete test suite
    asyncio.run(run_all_tests())