"""
Enhanced database connection manager for the loyalty points system.

This module provides an improved connection pool implementation with better
error handling, retry logic, health monitoring, and caching.
"""
import os
import logging
import threading
import time
import random
import json
import traceback
from typing import Dict, Optional, Any, Callable, Set, Tuple, TypeVar, Union, List
from contextlib import contextmanager
from urllib.parse import quote_plus
from datetime import datetime, timedelta
from pathlib import Path
from collections import OrderedDict

import sqlalchemy
from sqlalchemy import create_engine, text, func, event
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError, OperationalError, IntegrityError
from sqlalchemy.pool import QueuePool
from sqlalchemy.engine import Engine
import sqlite3

from path_utils import resolve_path
from models import Base, User, Membership, ControlRelationship, Transaction, InventoryItem, BuyerImmunity

# Configure logging
logger = logging.getLogger(__name__)

# Type variable for generic return types
T = TypeVar('T')

# Constants
DEFAULT_CACHE_SIZE = 200
DEFAULT_CACHE_TTL = 300  # 5 minutes in seconds
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 2  # seconds
MAX_POOL_SIZE = 40
DEFAULT_POOL_SIZE = 20
DEFAULT_MAX_OVERFLOW = 10
DEFAULT_POOL_TIMEOUT = 30
DEFAULT_POOL_RECYCLE = 1800  # 30 minutes
DEFAULT_CONNECT_TIMEOUT = 10
DEFAULT_STATEMENT_TIMEOUT = 30000  # 30 seconds

# Configure SQLite to handle concurrent requests better
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """Set SQLite connection pragmas for better performance and concurrency."""
    if isinstance(dbapi_connection, sqlite3.Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging
        cursor.execute("PRAGMA synchronous=NORMAL")  # Sync less frequently
        cursor.execute("PRAGMA cache_size=10000")  # Use more memory
        cursor.execute("PRAGMA temp_store=MEMORY")  # Store temp tables in memory
        cursor.execute("PRAGMA mmap_size=30000000000")  # Memory-mapped I/O
        cursor.execute("PRAGMA busy_timeout=30000")  # Wait up to 30s on locks
        cursor.close()

class RetryConfig:
    """Configuration for retry behavior."""
    
    def __init__(self, 
                max_retries: int = 3, 
                base_delay: float = 1.0,
                max_delay: float = 60.0,
                backoff_factor: float = 2.0,
                jitter: float = 0.1):
        """
        Initialize retry configuration.
        
        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay between retries in seconds
            max_delay: Maximum delay between retries in seconds
            backoff_factor: Multiplier for exponential backoff
            jitter: Random jitter factor (0-1) to add to delay
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        
    def get_delay(self, attempt: int) -> float:
        """
        Calculate delay for a specific retry attempt.
        
        Args:
            attempt: Current retry attempt (0-based)
            
        Returns:
            Delay in seconds
        """
        # Calculate exponential backoff
        delay = self.base_delay * (self.backoff_factor ** attempt)
        
        # Apply maximum cap
        delay = min(delay, self.max_delay)
        
        # Add jitter to prevent thundering herd
        if self.jitter > 0:
            jitter_amount = delay * self.jitter
            delay += random.uniform(-jitter_amount, jitter_amount)
            
        return max(0, delay)  # Ensure non-negative

class LRUCache:
    """A thread-safe LRU (Least Recently Used) cache implementation with TTL."""
    
    def __init__(self, max_size: int = DEFAULT_CACHE_SIZE, ttl: int = DEFAULT_CACHE_TTL):
        """
        Initialize the LRU cache.
        
        Args:
            max_size: Maximum number of items to store in the cache
            ttl: Time to live for cache entries in seconds
        """
        self.max_size = max_size
        self.ttl = ttl
        self.cache = OrderedDict()
        self.lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        self._last_stats_time = time.time()
        self._expire_count = 0
        
    def get(self, key: str) -> Optional[Any]:
        """
        Get an item from the cache if it exists and is not expired.
        
        Args:
            key: Cache key
            
        Returns:
            The cached value or None if not found or expired
        """
        with self.lock:
            if key not in self.cache:
                self._misses += 1
                self._log_stats_if_needed()
                return None
                
            # Check if item is expired
            value, timestamp = self.cache[key]
            if time.time() - timestamp > self.ttl:
                # Remove expired item
                del self.cache[key]
                self._misses += 1
                self._expire_count += 1
                self._log_stats_if_needed()
                return None
                
            # Move key to end to mark as recently used
            self.cache.move_to_end(key)
            self._hits += 1
            self._log_stats_if_needed()
            return value
            
    def set(self, key: str, value: Any) -> None:
        """
        Add or update an item in the cache.
        
        Args:
            key: Cache key
            value: Value to cache
        """
        with self.lock:
            # If key exists, update it
            if key in self.cache:
                self.cache.move_to_end(key)
                
            # Add new item with current timestamp
            self.cache[key] = (value, time.time())
            
            # Remove oldest item if cache is full
            if len(self.cache) > self.max_size:
                self.cache.popitem(last=False)
                
    def clear(self) -> None:
        """Clear all items from the cache."""
        with self.lock:
            self.cache.clear()
            
    def remove(self, key: str) -> None:
        """Remove a specific item from the cache."""
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                
    def __len__(self) -> int:
        """Return the number of items in the cache."""
        with self.lock:
            return len(self.cache)
    
    def _log_stats_if_needed(self):
        """Log cache statistics periodically."""
        now = time.time()
        if now - self._last_stats_time > 300:  # Every 5 minutes
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100) if total > 0 else 0
            logger.debug(f"Cache stats: {len(self.cache)}/{self.max_size} items, "
                        f"{hit_rate:.1f}% hit rate ({self._hits} hits, {self._misses} misses), "
                        f"{self._expire_count} expired")
            self._last_stats_time = now
            # Reset counters
            self._hits = 0
            self._misses = 0
            self._expire_count = 0

class ConnectionPoolStatus:
    """Status information for a database connection pool."""
    
    def __init__(self, checkedin: int, checkedout: int, total_connections: int):
        self.checkedin = checkedin
        self.checkedout = checkedout
        self.total_connections = total_connections
        
    def __str__(self) -> str:
        return f"Pool status: {self.checkedin} available, {self.checkedout} in use, {self.total_connections} total"

class EnhancedConnectionPool:
    """
    A more robust connection pool with better error handling and health monitoring.
    """
    
    def __init__(self, connection_string: str, pool_size: int = DEFAULT_POOL_SIZE, 
                 max_overflow: int = DEFAULT_MAX_OVERFLOW, pool_timeout: int = DEFAULT_POOL_TIMEOUT,
                 pool_recycle: int = DEFAULT_POOL_RECYCLE, connect_timeout: int = DEFAULT_CONNECT_TIMEOUT,
                 statement_timeout: int = DEFAULT_STATEMENT_TIMEOUT):
        """
        Initialize the connection pool.
        
        Args:
            connection_string: SQLAlchemy connection string
            pool_size: Number of connections to keep open
            max_overflow: Maximum number of connections beyond pool_size
            pool_timeout: Seconds to wait for a connection from the pool
            pool_recycle: Seconds before a connection is recycled
            connect_timeout: Seconds to wait for a new connection to be established
            statement_timeout: Milliseconds to wait for a statement to complete
        """
        self.connection_string = connection_string
        self.pool_size = min(pool_size, MAX_POOL_SIZE)
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle
        self.connect_timeout = connect_timeout
        self.statement_timeout = statement_timeout
        
        self.engine = None
        self.Session = None
        self.engine_lock = threading.Lock()
        
        # Health monitoring
        self.health_check_interval = 60  # seconds
        self.last_health_check = time.time() - (2 * self.health_check_interval)  # Force immediate check
        self.consecutive_failures = 0
        self.max_consecutive_failures = 3
        
        # Performance monitoring
        self.operation_count = 0
        self.operation_times = []
        self.last_stats_time = time.time()
        self.stats_interval = 300  # 5 minutes
        
        # Initialize the pool
        self.initialize()
        
    def initialize(self) -> bool:
        """
        Initialize the connection pool with better error recovery.
        """
        with self.engine_lock:
            try:
                if self.engine:
                    # Close existing connections
                    self.engine.dispose()
                    
                # Create the engine with connection pooling
                self.engine = create_engine(
                    self.connection_string,
                    poolclass=QueuePool,
                    pool_size=self.pool_size,
                    max_overflow=self.max_overflow,
                    pool_timeout=self.pool_timeout,
                    pool_recycle=self.pool_recycle,
                    pool_pre_ping=True,  # Check connection viability before using
                    connect_args={
                        'connect_timeout': self.connect_timeout
                    }
                )
                
                # Create session factory
                self.Session = scoped_session(sessionmaker(
                    bind=self.engine,
                    expire_on_commit=False  # Keep objects usable after commit
                ))
                
                # Try to connect and create tables if they don't exist
                try:
                    Base.metadata.create_all(self.engine)
                    
                    # Test the connection with a simple query
                    with self.engine.connect() as conn:
                        conn.execute(sqlalchemy.text("SELECT 1"))
                        
                    logger.info(f"Connection pool initialized with size={self.pool_size}, "
                            f"max_overflow={self.max_overflow}")
                    
                    # Reset failure counter on successful initialization
                    self.consecutive_failures = 0
                    
                    return True
                except Exception as conn_error:
                    logger.error(f"Failed to establish initial connection: {str(conn_error)}")
                    
                    # Try SQLite as fallback for development/testing
                    if 'postgresql' in self.connection_string:
                        logger.warning("Attempting to use SQLite as fallback database")
                        try:
                            sqlite_path = os.path.join(os.path.dirname(__file__), 'loyalty_points.db')
                            self.engine = create_engine(f"sqlite:///{sqlite_path}")
                            self.Session = scoped_session(sessionmaker(
                                bind=self.engine,
                                expire_on_commit=False
                            ))
                            Base.metadata.create_all(self.engine)
                            logger.info("Using SQLite as fallback database")
                            return True
                        except Exception as sqlite_error:
                            logger.error(f"Failed to initialize SQLite fallback: {str(sqlite_error)}")
                    
                    return False
            except Exception as e:
                logger.error(f"Failed to initialize connection pool: {str(e)}")
                self.engine = None
                self.Session = None
                
                # Increment failure counter
                self.consecutive_failures += 1
                
                return False
                
    def check_health(self, force: bool = False) -> bool:
        """
        Check the health of the connection pool.
        
        Args:
            force: If True, force a health check regardless of last check time
            
        Returns:
            True if pool is healthy, False otherwise
        """
        now = time.time()
        
        # Skip check if not forced and we checked recently
        if not force and now - self.last_health_check < self.health_check_interval:
            return True
            
        with self.engine_lock:
            self.last_health_check = now
            
            try:
                if not self.engine:
                    logger.warning("Connection pool not initialized")
                    return self.initialize()
                    
                # Verify connection by executing a simple query
                connection = self.engine.connect()
                connection.execute(text("SELECT 1"))
                connection.close()
                
                # Check pool status
                pool_status = self.get_pool_status()
                if pool_status:
                    logger.debug(f"Connection pool status: {pool_status}")
                    
                    # Check if we have connections available
                    if pool_status.checkedin == 0 and pool_status.checkedout == self.pool_size + self.max_overflow:
                        logger.warning("Connection pool at maximum capacity, consider increasing pool size")
                
                # Reset failure counter on successful health check
                self.consecutive_failures = 0
                
                return True
            except Exception as e:
                logger.error(f"Connection pool health check failed: {str(e)}")
                
                # Increment failure counter
                self.consecutive_failures += 1
                
                # Reinitialize if we've had too many consecutive failures
                if self.consecutive_failures >= self.max_consecutive_failures:
                    logger.warning(f"Reinitializing connection pool after {self.consecutive_failures} consecutive failures")
                    return self.initialize()
                
                return False
                
    def get_pool_status(self) -> Optional[ConnectionPoolStatus]:
        """
        Get current status of the connection pool.
        
        Returns:
            ConnectionPoolStatus object or None if status could not be determined
        """
        if not self.engine or not hasattr(self.engine, 'pool'):
            return None
            
        try:
            pool = self.engine.pool
            status = pool.status()
            
            if hasattr(status, 'checkedin') and hasattr(status, 'checkedout'):
                return ConnectionPoolStatus(
                    status.checkedin,
                    status.checkedout,
                    status.checkedin + status.checkedout
                )
            return None
        except Exception as e:
            logger.error(f"Error getting pool status: {str(e)}")
            return None
        
    def get_session(self):
        """
        Get a session from the pool.
        
        Returns:
            SQLAlchemy session
            
        Raises:
            Exception: If session could not be created
        """
        # Check pool health periodically
        self.check_health()
        
        if not self.Session:
            if not self.initialize():
                raise Exception("Failed to initialize database connection pool")
                
        return self.Session()
        
    def dispose(self):
        """Dispose of the connection pool."""
        with self.engine_lock:
            if self.engine:
                try:
                    self.engine.dispose()
                    logger.info("Connection pool disposed")
                except Exception as e:
                    logger.error(f"Error disposing connection pool: {str(e)}")
                finally:
                    self.engine = None
                    self.Session = None
                    
    def record_operation_time(self, duration_ms: float):
        """Record the time taken for a database operation."""
        self.operation_count += 1
        self.operation_times.append(duration_ms)
        
        # Log stats periodically
        self._log_stats_if_needed()
        
    def _log_stats_if_needed(self):
        """Log database performance statistics periodically."""
        now = time.time()
        if now - self.last_stats_time > self.stats_interval:
            # Calculate stats
            if self.operation_times:
                avg_time = sum(self.operation_times) / len(self.operation_times)
                max_time = max(self.operation_times)
                min_time = min(self.operation_times)
                p95 = sorted(self.operation_times)[int(len(self.operation_times) * 0.95)] if len(self.operation_times) > 20 else max_time
                
                logger.info(f"Database stats: {self.operation_count} operations, "
                           f"avg={avg_time:.2f}ms, min={min_time:.2f}ms, max={max_time:.2f}ms, p95={p95:.2f}ms")
                
                # Reset stats
                self.operation_times = []
                self.operation_count = 0
                
            self.last_stats_time = now

class TransactionManager:
    """
    Manages complex transactions spanning multiple tables and operations.
    
    This class provides tools for managing multi-step operations with proper
    error handling and rollback support.
    """
    
    def __init__(self, db_manager):
        """
        Initialize the transaction manager.
        
        Args:
            db_manager: Database manager instance
        """
        self.db = db_manager
        self.lock = threading.RLock()
        self.stats = {
            'successful_transactions': 0,
            'failed_transactions': 0,
            'rolled_back_transactions': 0,
            'average_duration_ms': 0,
            'total_duration_ms': 0
        }

    @contextmanager
    def transaction(self, description: str = "Database transaction"):
        """
        Create a transaction context for multi-step operations.
        """
        start_time = time.time()
        
        # Properly use the context manager with a with statement
        with self.db.get_session() as session:
            try:
                logger.debug(f"Starting transaction: {description}")
                
                # Yield to transaction block
                yield session
                
                # Stats update
                with self.lock:
                    self.stats['successful_transactions'] += 1
                    
                logger.debug(f"Transaction committed: {description}")
            except Exception as e:
                logger.warning(f"Transaction rolled back: {description} - {str(e)}")
                
                # Update stats
                with self.lock:
                    self.stats['rolled_back_transactions'] += 1
                    
                # Re-raise the original exception
                raise
            finally:
                # Update timing stats
                duration_ms = (time.time() - start_time) * 1000
                
                with self.lock:
                    self.stats['total_duration_ms'] += duration_ms
                    total_txn = (self.stats['successful_transactions'] + 
                            self.stats['failed_transactions'])
                    
                    if total_txn > 0:
                        self.stats['average_duration_ms'] = (
                            self.stats['total_duration_ms'] / total_txn
                        )
    
    def execute_in_transaction(self, func: Callable[..., T], description: str = None) -> T:
        """
        Execute a function within a transaction context.
        
        Args:
            func: Function to execute (receives session as first argument)
            description: Human-readable description of the transaction
            
        Returns:
            Result from the function
        """
        desc = description or func.__name__
        
        with self.transaction(desc) as session:
            return func(session)
            
    def get_stats(self) -> Dict[str, Any]:
        """
        Get transaction statistics.
        
        Returns:
            Dict of transaction statistics
        """
        with self.lock:
            return self.stats.copy()
    
    def reset_stats(self) -> None:
        """Reset transaction statistics."""
        with self.lock:
            for key in self.stats:
                self.stats[key] = 0

class DatabaseManager:
    """
    Enhanced database manager with improved connection pooling and error handling.
    This replaces the original DatabaseManager with more robust implementation.
    """
    
    def __init__(self, config_path: str, max_retries: int = DEFAULT_MAX_RETRIES):
        """Initialize the database manager with configuration."""
        self.max_retries = max_retries
        
        # Load configuration
        self.config = self._load_config(config_path)
        
        # User cache with improved LRU implementation
        self.user_cache = LRUCache(
            max_size=self.config.get('database', {}).get('cache_size', DEFAULT_CACHE_SIZE),
            ttl=self.config.get('database', {}).get('cache_ttl', DEFAULT_CACHE_TTL)
        )
        
        # Username cache for faster lookups
        self.username_cache = LRUCache(
            max_size=self.config.get('database', {}).get('cache_size', DEFAULT_CACHE_SIZE),
            ttl=self.config.get('database', {}).get('cache_ttl', DEFAULT_CACHE_TTL * 2)  # Longer TTL
        )
        
        # Control relationship cache
        self.control_cache = LRUCache(
            max_size=self.config.get('database', {}).get('cache_size', DEFAULT_CACHE_SIZE),
            ttl=60  # Shorter TTL since these change frequently
        )
        
        # Properly initialize references to model classes
        self.User = User
        self.Membership = Membership
        self.ControlRelationship = ControlRelationship
        self.Transaction = Transaction
        self.InventoryItem = InventoryItem
        self.BuyerImmunity = BuyerImmunity
        
        # Create connection pool
        connection_string = self._build_connection_string()
        db_config = self.config.get('database', {})
        self.connection_pool = EnhancedConnectionPool(
            connection_string,
            pool_size=db_config.get('pool_size', DEFAULT_POOL_SIZE),
            max_overflow=db_config.get('max_overflow', DEFAULT_MAX_OVERFLOW),
            pool_timeout=db_config.get('pool_timeout', DEFAULT_POOL_TIMEOUT),
            pool_recycle=db_config.get('pool_recycle', DEFAULT_POOL_RECYCLE)
        )
        
        # Initialize transaction manager
        self.transaction_manager = TransactionManager(self)
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from a JSON file."""
        try:
            # Resolve path
            config_path = resolve_path(config_path, must_exist=True)
            
            with open(config_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error in config file {config_path}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {str(e)}")
            raise
            
    def _build_connection_string(self) -> str:
        """
        Build a PostgreSQL connection string from config with environment variable overrides.
        """
        try:
            db_config = self.config.get('database', {})
            
            # Check for environment variables first
            username = os.environ.get('DB_USER') or quote_plus(str(db_config.get('user', 'postgres')))
            password = os.environ.get('DB_PASSWORD') or quote_plus(str(db_config.get('password', 'postgres')))
            host = os.environ.get('DB_HOST') or db_config.get('host', 'localhost')
            port = int(os.environ.get('DB_PORT') or db_config.get('port', 5432))
            db_name = os.environ.get('DB_NAME') or db_config.get('name', 'loyalty_points')
            
            return f"postgresql://{username}:{password}@{host}:{port}/{db_name}"
        except Exception as e:
            logger.error(f"Failed to build connection string: {str(e)}")
            raise
    
    @contextmanager
    def get_session(self):
        """Provide a transactional scope around operations with improved error handling."""
        max_retries = self.max_retries
        retry_count = 0
        session = None
        start_time = time.time()
        
        # Check connection pool health periodically
        self.connection_pool.check_health()
        
        while retry_count < max_retries:
            try:
                session = self.connection_pool.get_session()
                yield session
                session.commit()
                
                # Record operation time
                operation_time = (time.time() - start_time) * 1000  # ms
                self.connection_pool.record_operation_time(operation_time)
                
                return
            except OperationalError as e:
                # PostgreSQL server errors - retry these
                retry_count += 1
                if session:
                    session.rollback()
                    
                if retry_count >= max_retries:
                    logger.error(f"Database operation failed after {max_retries} retries: {str(e)}")
                    raise
                
                wait_time = (2 ** retry_count) + random.uniform(0, 0.5)
                logger.warning(
                    f"Database operational error (attempt {retry_count}/{max_retries}): {str(e)}, "
                    f"Retrying in {wait_time:.2f}s..."
                )
                time.sleep(wait_time)
            except IntegrityError as e:
                # Data integrity errors - these generally shouldn't be retried
                if session:
                    session.rollback()
                logger.error(f"Database integrity error: {str(e)}")
                raise
            except Exception as e:
                # For other errors, rollback and raise immediately
                if session:
                    session.rollback()
                logger.error(f"Database transaction error: {str(e)}")
                raise
            finally:
                if session:
                    session.close()
    
    def retry_operation(self, operation: Callable[[], T]) -> T:
        """Retry a database operation with exponential backoff."""
        retries = self.max_retries
        last_error = None
        
        for attempt in range(retries):
            try:
                return operation()
            except (OperationalError, IntegrityError) as e:
                last_error = e
                if attempt == retries - 1:
                    break
                    
                wait_time = (2 ** attempt) + random.uniform(0, 0.1)
                logger.warning(
                    f"Database operation failed (attempt {attempt+1}/{retries}), "
                    f"retrying in {wait_time:.2f}s: {str(e)}"
                )
                time.sleep(wait_time)
                
        logger.error(f"Operation failed after {retries} attempts: {str(last_error)}")
        raise last_error
    
    def get_user(self, user_id: str) -> Optional[User]:
        """Get a user by ID with caching."""
        # Check cache first
        cached_user = self.user_cache.get(user_id)
        if cached_user:
            return cached_user
            
        # Not in cache, fetch from database
        try:
            with self.get_session() as session:
                user = session.query(User).filter(User.id == user_id).first()
                if user:
                    # Store in both caches
                    self.user_cache.set(user_id, user)
                    self.username_cache.set(user.username.lower(), user)
                return user
        except Exception as e:
            logger.error(f"Error fetching user {user_id}: {str(e)}")
            return None
    
    def get_user_by_username(self, username: str) -> Optional[User]:
        """Get a user by username with caching."""
        if not username:
            return None
            
        # Normalize username for consistency
        normalized_username = username.lower()
        
        # Check cache first
        cached_user = self.username_cache.get(normalized_username)
        if cached_user:
            return cached_user
            
        # Not in cache, fetch from database
        with self.get_session() as session:
            # Use case-insensitive search
            user = session.query(User).filter(func.lower(User.username) == normalized_username).first()
            if user:
                # Store in both caches
                self.user_cache.set(user.id, user)
                self.username_cache.set(normalized_username, user)
            return user
        
    def get_active_users(self, minutes: int = 5) -> List[Dict[str, Any]]:
        """Get active users updated within the last N minutes, sorted by points."""
        with self.get_session() as session:
            cutoff_time = datetime.now() - timedelta(minutes=minutes)
            
            users = session.query(User).filter(
                User.updated_at >= cutoff_time
            ).order_by(User.points.desc()).all()
            
            result = []
            for user in users:
                result.append({
                    'id': user.id,
                    'username': user.username,
                    'points': user.points,
                    'updated_at': user.updated_at.isoformat() if user.updated_at else None
                })
                
            return result
    
    def create_user(self, user_id: str, username: str) -> User:
        """Create a new user."""
        with self.get_session() as session:
            user = User(id=user_id, username=username)
            session.add(user)
            session.commit()
            
            # Add to caches
            self.user_cache.set(user_id, user)
            self.username_cache.set(username.lower(), user)
            return user
    
    def get_or_create_user(self, user_id: str, username: str) -> User:
        """Get a user or create if doesn't exist."""
        # Check cache first
        cached_user = self.user_cache.get(user_id)
        if cached_user:
            return cached_user
            
        with self.get_session() as session:
            user = session.query(User).filter(User.id == user_id).first()
            if not user:
                user = User(id=user_id, username=username)
                session.add(user)
                session.commit()
                
            # Add to caches
            self.user_cache.set(user_id, user)
            self.username_cache.set(username.lower(), user)
            return user
    
    def update_user_points(
        self, 
        user_id: str, 
        points_delta: float, 
        reason: str = None, 
        source_transaction: str = None,
        is_control_distribution: bool = False
    ) -> float:
        """
        Update a user's points and record the transaction.
        
        Prevents points from going below zero.
        
        Returns:
            The actual points delta applied (may be different from requested if it would make points negative)
        """
        with self.transaction_manager.transaction("Update user points") as session:
            user = session.query(User).filter(User.id == user_id).first()
            if not user:
                logger.warning(f"Attempted to update points for non-existent user: {user_id}")
                return 0.0
                
            # Round to 1 decimal place for consistency
            points_delta = round(points_delta, 1)
            
            # Skip if zero delta (to avoid unnecessary transactions)
            if points_delta == 0:
                return 0.0
            
            # If user already has negative points, fix it first
            if user.points < 0:
                logger.warning(f"User {user_id} ({user.username}) has negative points ({user.points}). Fixing to 0.")
                fix_delta = -user.points
                
                # Record the fixing transaction
                fix_transaction = Transaction(
                    user_id=user_id,
                    points_change=fix_delta,
                    reason="Automatic adjustment to fix negative points",
                    source_transaction=None,
                    is_control_distribution=False
                )
                session.add(fix_transaction)
                
                # Update user points
                user.points = 0
                
            # Store original points for calculating actual delta
            original_points = user.points
            
            # Ensure points don't go below zero
            if points_delta < 0 and user.points + points_delta < 0:
                # Adjust delta to bring points to exactly zero
                points_delta = -user.points
                if abs(points_delta) < 1e-10:  # If adjustment is very small, consider it zero
                    return 0.0
                    
                logger.info(f"Adjusted point deduction for user {user_id} ({user.username}) to prevent negative balance. " +
                        f"Requested: {points_delta}, Adjusted to: {-user.points}")
                
            # Update points and ensure it's never negative
            user.points = max(0, round(user.points + points_delta, 1))
            user.updated_at = sqlalchemy.func.now()
            
            # Calculate the actual delta applied
            actual_delta = user.points - original_points
            
            # Skip transaction if no actual change
            if actual_delta == 0:
                return 0.0
            
            # Record transaction with the actual delta
            transaction = Transaction(
                user_id=user_id,
                points_change=actual_delta,
                reason=reason,
                source_transaction=source_transaction,
                is_control_distribution=is_control_distribution
            )
            session.add(transaction)
            
            # Update cache with new user state
            self.user_cache.set(user_id, user)
            
            return actual_delta  # Return the actual points change

    def fix_negative_points(self) -> int:
        """
        Find and fix users with negative points.
        Returns the number of users fixed.
        """
        with self.transaction_manager.transaction("Fix negative points") as session:
            # Find users with negative points
            users_with_negative_points = session.query(User).filter(User.points < 0).all()
            
            # Fix each user
            fixed_count = 0
            for user in users_with_negative_points:
                logger.warning(f"Found user with negative points: {user.username} ({user.id}): {user.points} points")
                
                # Create a transaction record for the adjustment
                adjustment = -user.points
                transaction = Transaction(
                    user_id=user.id,
                    points_change=adjustment,
                    reason="Automatic adjustment to fix negative points"
                )
                session.add(transaction)
                
                # Set points to zero
                user.points = 0
                user.updated_at = sqlalchemy.func.now()
                
                # Update cache with new user state
                self.user_cache.set(user.id, user)
                
                fixed_count += 1
                
            if fixed_count > 0:
                logger.info(f"Fixed {fixed_count} users with negative points")
                
            return fixed_count
    
    def get_control_relationship(self, target_id: str) -> Optional[ControlRelationship]:
        """Get control relationship by target ID with caching."""
        # Check cache first
        cached_rel = self.control_cache.get(f"target:{target_id}")
        if cached_rel is not None:
            if cached_rel == "none":  # Special marker for "no relationship"
                return None
            return cached_rel
            
        # Not in cache, fetch from database
        with self.get_session() as session:
            rel = session.query(ControlRelationship).filter(ControlRelationship.target_id == target_id).first()
            
            # Cache result (even if None)
            if rel:
                self.control_cache.set(f"target:{target_id}", rel)
            else:
                self.control_cache.set(f"target:{target_id}", "none")  # Special marker
                
            return rel
    
    def get_controlled_users(self, controller_id: str) -> List[ControlRelationship]:
        """Get all users controlled by a controller with caching."""
        # Cache key for the controller's targets
        cache_key = f"controller:{controller_id}"
        
        # Check cache first
        cached_rels = self.control_cache.get(cache_key)
        if cached_rels is not None:
            return cached_rels
            
        # Not in cache, fetch from database
        with self.get_session() as session:
            rels = session.query(ControlRelationship).filter(
                ControlRelationship.controller_id == controller_id
            ).all()
            
            # Cache the result
            self.control_cache.set(cache_key, rels)
            
            return rels
        
    def get_user_membership(self, user_id: str) -> Optional[Membership]:
        """Get a user's membership status with caching."""
        # Check cache first
        cache_key = f"membership:{user_id}"
        cached_membership = self.control_cache.get(cache_key)
        if cached_membership is not None:
            if cached_membership == "none":  # Special marker for "no membership"
                return None
            return cached_membership
            
        # Not in cache, fetch from database
        with self.get_session() as session:
            membership = session.query(Membership).filter(Membership.user_id == user_id).first()
            
            # Cache the result (even if None)
            if membership:
                self.control_cache.set(cache_key, membership)
            else:
                self.control_cache.set(cache_key, "none")  # Special marker
                
            return membership

    def update_user_membership(self, user_id: str, months_subscribed: float) -> None:
        """Update a user's membership status and calculate the multiplier."""
        with self.transaction_manager.transaction("Update user membership") as session:
            membership = session.query(Membership).filter(Membership.user_id == user_id).first()
            
            # Calculate multiplier: Member for X months = (X+1) multiplier
            multiplier = int(months_subscribed) + 1
            
            if membership:
                membership.months_subscribed = months_subscribed
                membership.multiplier = multiplier
                membership.last_updated = sqlalchemy.func.now()
            else:
                # Check if user exists
                user = session.query(User).filter(User.id == user_id).first()
                if not user:
                    logger.warning(f"Attempted to update membership for non-existent user: {user_id}")
                    return
                    
                membership = Membership(
                    user_id=user_id,
                    months_subscribed=months_subscribed,
                    multiplier=multiplier
                )
                session.add(membership)
                
            # Update cache
            cache_key = f"membership:{user_id}"
            if membership:
                self.control_cache.set(cache_key, membership)
            else:
                self.control_cache.set(cache_key, "none")
                
            logger.debug(f"Updated membership for user {user_id}: {months_subscribed} months, x{multiplier} multiplier")
        
    def has_buyer_immunity(self, target_id: str, buyer_id: str) -> bool:
        """Check if a target has immunity against a specific buyer with caching."""
        # Create a cache key for this immunity relationship
        cache_key = f"immunity:{target_id}:{buyer_id}"
        
        # Check cache first
        cached_immunity = self.control_cache.get(cache_key)
        if cached_immunity is not None:
            return cached_immunity  # Will be True or False
        
        # Not in cache, fetch from database
        with self.get_session() as session:
            immunity = session.query(BuyerImmunity).filter(
                BuyerImmunity.target_id == target_id,
                BuyerImmunity.buyer_id == buyer_id
            ).first()
            
            result = immunity is not None
            
            # Cache the result (True or False)
            self.control_cache.set(cache_key, result)
            
            return result

    def add_buyer_immunity(self, target_id: str, buyer_id: str) -> bool:
        """Add buyer immunity for a target with caching."""
        with self.transaction_manager.transaction("Add buyer immunity") as session:
            # Check if immunity already exists
            existing = session.query(BuyerImmunity).filter(
                BuyerImmunity.target_id == target_id,
                BuyerImmunity.buyer_id == buyer_id
            ).first()
            
            if existing:
                # Update cache to reflect immunity exists
                self.control_cache.set(f"immunity:{target_id}:{buyer_id}", True)
                return True
                
            # Check if both users exist
            target = session.query(User).filter(User.id == target_id).first()
            buyer = session.query(User).filter(User.id == buyer_id).first()
            
            if not target or not buyer:
                logger.warning(f"Target {target_id} or buyer {buyer_id} does not exist")
                return False
                
            # Create immunity
            immunity = BuyerImmunity(
                target_id=target_id,
                buyer_id=buyer_id
            )
            session.add(immunity)
            
            # Update cache
            self.control_cache.set(f"immunity:{target_id}:{buyer_id}", True)
            
            logger.info(f"Added buyer immunity: {target.username} is now immune to {buyer.username}")
            return True

    def remove_buyer_immunity(self, target_id: str, buyer_id: str = None) -> bool:
        """Remove buyer immunity for a target with cache invalidation."""
        with self.transaction_manager.transaction("Remove buyer immunity") as session:
            query = session.query(BuyerImmunity).filter(BuyerImmunity.target_id == target_id)
            
            # If a specific buyer is specified, only remove immunity against that buyer
            if buyer_id:
                query = query.filter(BuyerImmunity.buyer_id == buyer_id)
                
            # Get all matching immunities
            immunities = query.all()
            if not immunities:
                return False
                
            # Delete the immunities
            for immunity in immunities:
                specific_buyer_id = immunity.buyer_id
                session.delete(immunity)
                
                # Invalidate specific immunity cache
                self.control_cache.remove(f"immunity:{target_id}:{specific_buyer_id}")
            
            # Log the action
            logger.info(f"Removed buyer immunity for target {target_id}" + 
                    (f" against buyer {buyer_id}" if buyer_id else " against all buyers"))
            return True
    
    def create_control_relationship(
        self, controller_id: str, target_id: str, control_percent: float = 40.0
    ) -> Optional[ControlRelationship]:
        """Create a new control relationship."""
        with self.transaction_manager.transaction("Create control relationship") as session:
            # Check if target is already controlled
            existing = session.query(ControlRelationship).filter(
                ControlRelationship.target_id == target_id
            ).first()
            
            if existing:
                logger.warning(f"Target {target_id} is already controlled by {existing.controller_id}")
                return None
                
            # Check if controller and target exist
            controller = session.query(User).filter(User.id == controller_id).first()
            target = session.query(User).filter(User.id == target_id).first()
            
            if not controller or not target:
                logger.warning(f"Controller {controller_id} or target {target_id} does not exist")
                return None
                
            # Create relationship with explicit initial timestamps
            relationship = ControlRelationship(
                controller_id=controller_id,
                target_id=target_id,
                control_percent=control_percent,
                start_time=sqlalchemy.func.now(),
                last_checkin=sqlalchemy.func.now()
            )
            session.add(relationship)
            
            # Update control relationship caches
            self.control_cache.set(f"target:{target_id}", relationship)
            
            # Invalidate controller's targets cache
            self.control_cache.remove(f"controller:{controller_id}")
            
            logger.info(f"Created control relationship: {controller.username} -> {target.username}")
            return relationship
    
    def remove_control_relationship(self, target_id: str) -> bool:
        """Remove a control relationship by target ID."""
        with self.transaction_manager.transaction("Remove control relationship") as session:
            relationship = session.query(ControlRelationship).filter(
                ControlRelationship.target_id == target_id
            ).first()
            
            if relationship:
                controller_id = relationship.controller_id
                
                # Delete the relationship
                session.delete(relationship)
                
                # Update caches
                self.control_cache.set(f"target:{target_id}", "none")  # Mark as no relationship
                self.control_cache.remove(f"controller:{controller_id}")  # Invalidate controller's targets
                
                return True
            return False
    
    def update_control_checkin(self, controller_id: str, target_id: str) -> bool:
        """Update the last checkin time for a control relationship."""
        try:
            with self.transaction_manager.transaction("Update control checkin") as session:
                # Find the relationship
                rel = session.query(ControlRelationship).filter(
                    ControlRelationship.controller_id == controller_id,
                    ControlRelationship.target_id == target_id
                ).first()
                
                if not rel:
                    return False
                    
                # Update the last checkin time
                rel.last_checkin = sqlalchemy.func.now()
                
                # Update cache if it exists
                cached_rel = self.control_cache.get(f"target:{target_id}")
                if cached_rel and cached_rel != "none":
                    cached_rel.last_checkin = rel.last_checkin
                    self.control_cache.set(f"target:{target_id}", cached_rel)
                
                return True
        except Exception as e:
            logger.error(f"Error updating check-in time: {e}")
            return False
    
    def get_all_control_relationships(self):
        """Get all control relationships in the system with improved reliability."""
        with self.get_session() as session:
            relationships = session.query(self.ControlRelationship).all()
            
            # Enhanced logging for debugging control relationships
            if relationships:
                logger.info(f"Retrieved {len(relationships)} control relationships:")
                for rel in relationships:
                    controller = self.get_user(rel.controller_id)
                    target = self.get_user(rel.target_id)
                    controller_name = controller.username if controller else rel.controller_id
                    target_name = target.username if target else rel.target_id
                    logger.debug(f"  {controller_name} ({rel.controller_id}) controls {target_name} ({rel.target_id})")
                    
                    # Ensure cache is synchronized with database
                    self.control_cache.set(f"target:{rel.target_id}", rel)
            else:
                logger.debug("No control relationships found in database")
                
            return relationships
    
    def get_expired_control_relationships(self, minutes: int = 15) -> List[ControlRelationship]:
        """Get control relationships that haven't been checked in within the specified time."""
        with self.get_session() as session:
            cutoff_time = datetime.now() - timedelta(minutes=minutes)
            
            expired = session.query(ControlRelationship).filter(
                ControlRelationship.last_checkin < cutoff_time
            ).all()
            
            if expired:
                logger.info(f"Found {len(expired)} expired control relationships")
                
            return expired
    
    def is_user_eliminated(self, user_id: str) -> bool:
        """Check if a user is eliminated."""
        # Check cache first
        cached_user = self.user_cache.get(user_id)
        if cached_user:
            return cached_user.is_eliminated
            
        with self.get_session() as session:
            user = session.query(User).filter(User.id == user_id).first()
            return user.is_eliminated if user else False
    
    def eliminate_user(self, user_id: str, reason: str = None) -> bool:
        """Mark a user as eliminated."""
        with self.transaction_manager.transaction("Eliminate user") as session:
            user = session.query(User).filter(User.id == user_id).first()
            if not user:
                return False
                
            # Skip if already eliminated
            if user.is_eliminated:
                return True
                
            user.is_eliminated = True
            user.elimination_reason = reason
            
            # Update cache
            self.user_cache.set(user_id, user)
            
            # Log elimination
            logger.info(f"User {user_id} ({user.username}) eliminated: {reason}")
            return True
    
    def revive_user(self, user_id: str) -> bool:
        """Revive an eliminated user."""
        with self.transaction_manager.transaction("Revive user") as session:
            user = session.query(User).filter(User.id == user_id).first()
            if not user:
                return False
                
            # Skip if not eliminated
            if not user.is_eliminated:
                return True
                
            user.is_eliminated = False
            user.elimination_reason = None
            
            # Update cache
            self.user_cache.set(user_id, user)
            
            # Log revival
            logger.info(f"User {user_id} ({user.username}) revived")
            return True
    
    def add_to_inventory(self, user_id: str, item_type: str, quantity: int = 1) -> bool:
        """Add an item to a user's inventory."""
        with self.transaction_manager.transaction("Add to inventory") as session:
            # Get existing inventory item
            inventory_item = session.query(InventoryItem).filter(
                InventoryItem.user_id == user_id,
                InventoryItem.item_type == item_type
            ).first()
            
            if inventory_item:
                inventory_item.quantity += quantity
                logger.debug(f"Added {quantity} {item_type} to user {user_id}'s inventory (new total: {inventory_item.quantity})")
            else:
                # Check if user exists
                user = session.query(User).filter(User.id == user_id).first()
                if not user:
                    logger.warning(f"Attempted to add item to inventory for non-existent user: {user_id}")
                    return False
                    
                inventory_item = InventoryItem(
                    user_id=user_id,
                    item_type=item_type,
                    quantity=quantity
                )
                session.add(inventory_item)
                logger.debug(f"Created new inventory entry with {quantity} {item_type} for user {user_id}")
                
            return True
    
    def use_inventory_item(self, user_id: str, item_type: str, quantity: int = 1) -> bool:
        """
        Use an item from a user's inventory.
        
        Args:
            user_id: User ID
            item_type: Type of item to use
            quantity: Quantity to use (default: 1)
            
        Returns:
            True if successful, False if user doesn't have enough items
        """
        with self.transaction_manager.transaction("Use inventory item") as session:
            inventory_item = session.query(InventoryItem).filter(
                InventoryItem.user_id == user_id,
                InventoryItem.item_type == item_type
            ).first()
            
            if not inventory_item or inventory_item.quantity < quantity:
                logger.debug(f"User {user_id} tried to use {quantity} {item_type} but has insufficient quantity")
                return False
                
            inventory_item.quantity -= quantity
            inventory_item.last_used = sqlalchemy.func.now()
            
            logger.debug(f"User {user_id} used {quantity} {item_type} (remaining: {inventory_item.quantity})")
            
            # Remove item if quantity is zero
            if inventory_item.quantity <= 0:
                session.delete(inventory_item)
                logger.debug(f"Removed empty inventory entry for {item_type} from user {user_id}")
                
            return True
    
    def get_inventory(self, user_id: str) -> List[InventoryItem]:
        """Get all items in a user's inventory."""
        with self.get_session() as session:
            return session.query(InventoryItem).filter(InventoryItem.user_id == user_id).all()
    
    def get_user_statistics(self) -> Dict[str, Any]:
        """Get system-wide statistics about users."""
        with self.get_session() as session:
            # Use more efficient count queries
            total_users = session.query(func.count(User.id)).scalar()
            active_users = session.query(func.count(User.id)).filter(
                User.updated_at >= datetime.now() - timedelta(days=7)
            ).scalar()
            eliminated_users = session.query(func.count(User.id)).filter(
                User.is_eliminated == True
            ).scalar()
            
            # Get total points using sum
            total_points = session.query(func.sum(User.points)).scalar() or 0
            
            # Count relationships and items
            control_relationships = session.query(func.count(ControlRelationship.controller_id)).scalar()
            inventory_items = session.query(func.sum(InventoryItem.quantity)).scalar() or 0
            
            return {
                'total_users': total_users,
                'active_users': active_users,
                'eliminated_users': eliminated_users,
                'total_points': round(total_points, 1),
                'control_relationships': control_relationships,
                'inventory_items': inventory_items,
                'timestamp': datetime.now().isoformat()
            }
    
    def get_top_users(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the top users by points."""
        with self.get_session() as session:
            # Use a single efficient query with joins
            users_with_relations = session.query(
                User,
                func.count(ControlRelationship.target_id).label('target_count')
            ).outerjoin(
                ControlRelationship,
                ControlRelationship.controller_id == User.id
            ).group_by(User.id).order_by(User.points.desc()).limit(limit).all()
            
            # Get target relationships in a separate query for efficiency
            target_ids = [user.id for user, _ in users_with_relations]
            control_map = {}
            
            if target_ids:
                control_results = session.query(
                    ControlRelationship.target_id
                ).filter(
                    ControlRelationship.target_id.in_(target_ids)
                ).all()
                
                for result in control_results:
                    control_map[result.target_id] = True
            
            # Build result list
            result = []
            for user, target_count in users_with_relations:
                result.append({
                    'id': user.id,
                    'username': user.username,
                    'points': user.points,
                    'is_eliminated': user.is_eliminated,
                    'is_target': user.id in control_map,
                    'targets_count': target_count
                })
                
            return result
    
    def clear_all_control_relationships(self) -> int:
        """Clear all control relationships and related caches. Returns the number of relationships cleared."""
        with self.transaction_manager.transaction("Clear all control relationships") as session:
            count = session.query(self.ControlRelationship).delete()
            
            # Clear control cache completely
            self.control_cache.clear()
            
            logger.info(f"Cleared {count} control relationships and control cache")
            return count
    
    def clear_all_elimination_statuses(self) -> int:
        """Clear all user elimination statuses. Returns the number of users affected."""
        with self.transaction_manager.transaction("Clear all elimination statuses") as session:
            count = session.query(self.User).filter(self.User.is_eliminated == True).update(
                {self.User.is_eliminated: False, self.User.elimination_reason: None}
            )
            
            # Update cache for affected users
            if count > 0:
                # Get IDs of previously eliminated users
                eliminated_user_ids = session.query(self.User.id).filter(
                    self.User.is_eliminated == False,  # Users that were just updated
                    self.User.elimination_reason == None
                ).all()
                
                # Clear them from cache to force reload
                for user_id, in eliminated_user_ids:
                    self.user_cache.remove(user_id)
                    
            logger.info(f"Cleared elimination status for {count} users")
            return count
    
    def close(self) -> None:
        """Close database connections."""
        # Clear caches
        self.user_cache.clear()
        self.username_cache.clear()
        self.control_cache.clear()
        
        # Dispose of connection pool
        if hasattr(self, 'connection_pool'):
            self.connection_pool.dispose()
            
        logger.info("Database connections closed")
    
    def __del__(self) -> None:
        """Destructor to ensure connections are closed."""
        try:
            self.close()
        except Exception as e:
            logger.error(f"Error during database connection cleanup: {str(e)}")
