"""
Database connection and management for Singapore Company Database
Handles PostgreSQL connections, connection pooling, and database operations
"""

import asyncio
import asyncpg
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager, asynccontextmanager
from typing import Optional, Dict, Any, List, AsyncGenerator, Generator
import pandas as pd
from datetime import datetime
import json

from src.config import settings
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self):
        self.engine = None
        self.session_factory = None
        self.connection_pool = None
        self.async_pool = None
        self._initialized = False
    
    def initialize(self):
        """Initialize database connections and pools"""
        if self._initialized:
            return
        
        try:
            # SQLAlchemy engine with connection pooling
            self.engine = create_engine(
                settings.database.url,
                poolclass=QueuePool,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=settings.debug
            )
            
            # Session factory
            self.session_factory = sessionmaker(bind=self.engine)
            
            # psycopg2 connection pool for direct operations
            self.connection_pool = ThreadedConnectionPool(
                minconn=2,
                maxconn=10,
                dsn=settings.database.url
            )
            
            self._initialized = True
            logger.info("Database connections initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database connections: {e}")
            raise
    
    async def initialize_async(self):
        """Initialize async database connection pool"""
        try:
            self.async_pool = await asyncpg.create_pool(
                settings.database.url,
                min_size=2,
                max_size=10,
                command_timeout=60
            )
            logger.info("Async database pool initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize async database pool: {e}")
            raise
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Get SQLAlchemy session with automatic cleanup"""
        if not self._initialized:
            self.initialize()
        
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    @contextmanager
    def get_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """Get raw psycopg2 connection with automatic cleanup"""
        if not self._initialized:
            self.initialize()
        
        connection = self.connection_pool.getconn()
        try:
            yield connection
            connection.commit()
        except Exception as e:
            connection.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            self.connection_pool.putconn(connection)
    
    @asynccontextmanager
    async def get_async_connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Get async connection with automatic cleanup"""
        if not self.async_pool:
            await self.initialize_async()
        
        async with self.async_pool.acquire() as connection:
            try:
                yield connection
            except Exception as e:
                logger.error(f"Async database connection error: {e}")
                raise
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute query and return results as list of dictionaries"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params or {})
                if cursor.description:
                    return [dict(row) for row in cursor.fetchall()]
                return []
    
    async def execute_async_query(self, query: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        """Execute async query and return results"""
        async with self.get_async_connection() as conn:
            rows = await conn.fetch(query, *(params or []))
            return [dict(row) for row in rows]
    
    def execute_batch_insert(self, table: str, data: List[Dict[str, Any]], 
                           batch_size: int = 1000, on_conflict: str = "DO NOTHING") -> int:
        """Execute batch insert with conflict resolution"""
        if not data:
            return 0
        
        total_inserted = 0
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                # Get column names from first record
                columns = list(data[0].keys())
                placeholders = ', '.join(['%s'] * len(columns))
                column_names = ', '.join(columns)
                
                # Build insert query
                query = f"""
                    INSERT INTO {table} ({column_names}) 
                    VALUES ({placeholders})
                    ON CONFLICT {on_conflict}
                """
                
                # Process in batches
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    values = [tuple(row[col] for col in columns) for row in batch]
                    
                    try:
                        cursor.executemany(query, values)
                        batch_inserted = cursor.rowcount
                        total_inserted += batch_inserted
                        
                        logger.info(f"Inserted batch {i//batch_size + 1}: {batch_inserted} records")
                        
                    except Exception as e:
                        logger.error(f"Error inserting batch {i//batch_size + 1}: {e}")
                        conn.rollback()
                        raise
        
        logger.info(f"Total records inserted into {table}: {total_inserted}")
        return total_inserted
    
    def upsert_records(self, table: str, data: List[Dict[str, Any]], 
                      conflict_columns: List[str], update_columns: List[str] = None) -> int:
        """Upsert records with ON CONFLICT DO UPDATE"""
        if not data:
            return 0
        
        if update_columns is None:
            update_columns = [col for col in data[0].keys() if col not in conflict_columns]
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                columns = list(data[0].keys())
                placeholders = ', '.join(['%s'] * len(columns))
                column_names = ', '.join(columns)
                conflict_cols = ', '.join(conflict_columns)
                
                # Build update clause
                update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
                
                query = f"""
                    INSERT INTO {table} ({column_names}) 
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_cols}) 
                    DO UPDATE SET {update_clause}
                """
                
                values = [tuple(row[col] for col in columns) for row in data]
                cursor.executemany(query, values)
                
                affected_rows = cursor.rowcount
                logger.info(f"Upserted {affected_rows} records in {table}")
                return affected_rows
    
    def bulk_copy_from_dataframe(self, df: pd.DataFrame, table: str, 
                                if_exists: str = 'append') -> int:
        """Bulk copy DataFrame to database table using pandas"""
        if df.empty:
            return 0
        
        try:
            records_inserted = df.to_sql(
                table, 
                self.engine, 
                if_exists=if_exists, 
                index=False, 
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"Bulk copied {len(df)} records to {table}")
            return len(df)
            
        except Exception as e:
            logger.error(f"Error bulk copying to {table}: {e}")
            raise
    
    def get_table_stats(self, table: str) -> Dict[str, Any]:
        """Get table statistics"""
        query = """
            SELECT 
                schemaname,
                tablename,
                n_tup_ins as inserts,
                n_tup_upd as updates,
                n_tup_del as deletes,
                n_live_tup as live_tuples,
                n_dead_tup as dead_tuples,
                last_vacuum,
                last_autovacuum,
                last_analyze,
                last_autoanalyze
            FROM pg_stat_user_tables 
            WHERE tablename = %s
        """
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, (table,))
                result = cursor.fetchone()
                return dict(result) if result else {}
    
    def get_database_size(self) -> Dict[str, Any]:
        """Get database size information"""
        query = """
            SELECT 
                pg_database.datname as database_name,
                pg_size_pretty(pg_database_size(pg_database.datname)) as size,
                pg_database_size(pg_database.datname) as size_bytes
            FROM pg_database 
            WHERE datname = %s
        """
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, (settings.database.name,))
                result = cursor.fetchone()
                return dict(result) if result else {}
    
    def get_table_sizes(self) -> List[Dict[str, Any]]:
        """Get sizes of all tables"""
        query = """
            SELECT 
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
            FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        """
        
        return self.execute_query(query)
    
    def vacuum_analyze_table(self, table: str):
        """Run VACUUM ANALYZE on a table"""
        with self.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(f"VACUUM ANALYZE {table}")
                logger.info(f"VACUUM ANALYZE completed for {table}")
    
    def create_index_if_not_exists(self, index_name: str, table: str, 
                                  columns: List[str], index_type: str = "btree"):
        """Create index if it doesn't exist"""
        column_list = ', '.join(columns)
        query = f"""
            CREATE INDEX IF NOT EXISTS {index_name} 
            ON {table} USING {index_type} ({column_list})
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                logger.info(f"Index {index_name} created/verified on {table}")
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get database connection information"""
        query = """
            SELECT 
                current_database() as database,
                current_user as user,
                inet_server_addr() as server_addr,
                inet_server_port() as server_port,
                version() as version
        """
        
        result = self.execute_query(query)
        return result[0] if result else {}
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result[0] == 1
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    async def test_async_connection(self) -> bool:
        """Test async database connection"""
        try:
            async with self.get_async_connection() as conn:
                result = await conn.fetchval("SELECT 1")
                return result == 1
        except Exception as e:
            logger.error(f"Async database connection test failed: {e}")
            return False
    
    def close_connections(self):
        """Close all database connections"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Closed psycopg2 connection pool")
        
        if self.engine:
            self.engine.dispose()
            logger.info("Closed SQLAlchemy engine")
    
    async def close_async_connections(self):
        """Close async database connections"""
        if self.async_pool:
            await self.async_pool.close()
            logger.info("Closed async connection pool")


# Global database manager instance
db_manager = DatabaseManager()


# Convenience functions
def get_db_session():
    """Get database session"""
    return db_manager.get_session()


def get_db_connection():
    """Get database connection"""
    return db_manager.get_connection()


async def get_async_db_connection():
    """Get async database connection"""
    return db_manager.get_async_connection()


def execute_query(query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Execute query and return results"""
    return db_manager.execute_query(query, params)


async def execute_async_query(query: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
    """Execute async query and return results"""
    return await db_manager.execute_async_query(query, params)


def batch_insert(table: str, data: List[Dict[str, Any]], **kwargs) -> int:
    """Batch insert data"""
    return db_manager.execute_batch_insert(table, data, **kwargs)


def upsert_records(table: str, data: List[Dict[str, Any]], 
                  conflict_columns: List[str], **kwargs) -> int:
    """Upsert records"""
    return db_manager.upsert_records(table, data, conflict_columns, **kwargs)


# Database health check
class DatabaseHealthCheck:
    """Database health monitoring"""
    
    @staticmethod
    def check_database_health() -> Dict[str, Any]:
        """Comprehensive database health check"""
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'connection_test': False,
            'async_connection_test': False,
            'database_size': {},
            'table_stats': {},
            'connection_info': {},
            'issues': []
        }
        
        try:
            # Test connections
            health_status['connection_test'] = db_manager.test_connection()
            
            if health_status['connection_test']:
                # Get database info
                health_status['connection_info'] = db_manager.get_connection_info()
                health_status['database_size'] = db_manager.get_database_size()
                
                # Check main tables
                main_tables = ['companies', 'company_social_media', 'company_financials']
                for table in main_tables:
                    stats = db_manager.get_table_stats(table)
                    if stats:
                        health_status['table_stats'][table] = stats
                    else:
                        health_status['issues'].append(f"Table {table} not found or no stats available")
            
        except Exception as e:
            health_status['issues'].append(f"Health check error: {str(e)}")
        
        return health_status
    
    @staticmethod
    async def check_async_health() -> bool:
        """Check async connection health"""
        try:
            return await db_manager.test_async_connection()
        except Exception:
            return False


# Initialize database on module import
if not settings.testing:
    try:
        db_manager.initialize()
    except Exception as e:
        logger.warning(f"Could not initialize database on import: {e}")


# Example usage and testing
if __name__ == "__main__":
    import asyncio
    
    async def test_database():
        """Test database functionality"""
        print("Testing database connections...")
        
        # Test sync connection
        sync_test = db_manager.test_connection()
        print(f"Sync connection test: {'PASS' if sync_test else 'FAIL'}")
        
        # Test async connection
        async_test = await db_manager.test_async_connection()
        print(f"Async connection test: {'PASS' if async_test else 'FAIL'}")
        
        if sync_test:
            # Get connection info
            info = db_manager.get_connection_info()
            print(f"Connected to: {info.get('database')} as {info.get('user')}")
            
            # Get database size
            size_info = db_manager.get_database_size()
            print(f"Database size: {size_info.get('size', 'Unknown')}")
            
            # Test query execution
            result = execute_query("SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = 'public'")
            print(f"Public tables count: {result[0]['count'] if result else 0}")
        
        # Health check
        health = DatabaseHealthCheck.check_database_health()
        print(f"Health check issues: {len(health['issues'])}")
        
        print("Database test completed!")
    
    asyncio.run(test_database())
