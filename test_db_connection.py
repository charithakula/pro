"""
Standalone Database Connection Test Script
Tests PostgreSQL connection independently without Flask context
Useful for debugging database connectivity issues
"""

import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv

# ============================================================================
# SETUP LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('db_test.log')
    ]
)

logger = logging.getLogger(__name__)

# ============================================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================================

load_dotenv()

logger.info("=" * 80)
logger.info("DATABASE CONNECTION TEST")
logger.info("=" * 80)
logger.info(f"Test started at: {datetime.now().isoformat()}")


# ============================================================================
# DATABASE CONNECTOR CLASS (from document)
# ============================================================================

class DatabaseConnectorError(Exception):
    """Custom exception for database connector errors"""
    pass


class DatabaseConnector:
    """
    Database connector for PostgreSQL with schema support
    Manages connections, pooling, and health checks
    """
    
    def __init__(self, config=None):
        """Initialize database connector with configuration"""
        try:
            # Get configuration
            if config is None:
                config = self._get_config()
            
            # Extract configuration
            self.db_type = self._get_config_value(config, 'DB_TYPE', 'postgresql')
            self.db_host = self._get_config_value(config, 'DB_HOST', 'localhost')
            self.db_port = str(self._get_config_value(config, 'DB_PORT', 5434))
            self.db_name = self._get_config_value(config, 'DB_NAME', 'dashboard_360')
            self.db_user = self._get_config_value(config, 'DB_USER', 'dashboard_user')
            self.db_password = self._get_config_value(config, 'DB_PASSWORD', '')
            self.db_sslmode = self._get_config_value(config, 'DB_SSLMODE', 'disable')
            self.db_schema = self._get_config_value(config, 'DB_SCHEMA', 'igpt')
            
            # SQLAlchemy pool configuration
            self.pool_size = int(self._get_config_value(config, 'SQLALCHEMY_POOL_SIZE', 10))
            self.pool_recycle = int(self._get_config_value(config, 'SQLALCHEMY_POOL_RECYCLE', 3600))
            self.pool_timeout = int(self._get_config_value(config, 'SQLALCHEMY_POOL_TIMEOUT', 30))
            self.pool_pre_ping = self._get_config_value(config, 'SQLALCHEMY_POOL_PRE_PING', True)
            
            # Validate database type
            if self.db_type != 'postgresql':
                raise DatabaseConnectorError(
                    f"Unsupported database type: {self.db_type}. Only PostgreSQL is supported."
                )
            
            # Build connection string
            self.connection_string = self._build_connection_string()
            self.is_connected = False
            self.connection_pool = None
            
            logger.info("✓ Database Connector initialized (PostgreSQL)")
            logger.info(f"  - Host: {self.db_host}:{self.db_port}")
            logger.info(f"  - Database: {self.db_name}")
            logger.info(f"  - Schema: {self.db_schema}")
            logger.info(f"  - User: {self.db_user}")
            logger.info(f"  - Pool Size: {self.pool_size}")
            
        except DatabaseConnectorError:
            raise
        except Exception as e:
            logger.error(f"✗ Failed to initialize Database Connector: {e}")
            raise DatabaseConnectorError(f"Initialization failed: {str(e)}")
    
    
    @staticmethod
    def _get_config():
        """Get configuration from environment variables"""
        config = {
            'DB_TYPE': os.getenv('DB_TYPE', 'postgresql'),
            'DB_HOST': os.getenv('DB_HOST', 'localhost'),
            'DB_PORT': os.getenv('DB_PORT', '5434'),
            'DB_NAME': os.getenv('DB_NAME', 'dashboard_360'),
            'DB_USER': os.getenv('DB_USER', 'dashboard_user'),
            'DB_PASSWORD': os.getenv('DB_PASSWORD', ''),
            'DB_SSLMODE': os.getenv('DB_SSLMODE', 'disable'),
            'DB_SCHEMA': os.getenv('DB_SCHEMA', 'igpt'),
            'SQLALCHEMY_POOL_SIZE': os.getenv('SQLALCHEMY_POOL_SIZE', '10'),
            'SQLALCHEMY_POOL_RECYCLE': os.getenv('SQLALCHEMY_POOL_RECYCLE', '3600'),
            'SQLALCHEMY_POOL_TIMEOUT': os.getenv('SQLALCHEMY_POOL_TIMEOUT', '30'),
            'SQLALCHEMY_POOL_PRE_PING': os.getenv('SQLALCHEMY_POOL_PRE_PING', 'True')
        }
        logger.info("✓ Using environment variables for configuration")
        return config
    
    
    @staticmethod
    def _get_config_value(config, key, default):
        """Safely get configuration value"""
        if isinstance(config, dict):
            value = config.get(key)
            if value is not None:
                return value
        
        try:
            value = getattr(config, key, None)
            if value is not None:
                return value
        except:
            pass
        
        return default
    
    
    def _build_connection_string(self):
        """Build PostgreSQL connection string"""
        try:
            connection_string = (
                f"postgresql://{self.db_user}:{self.db_password}@"
                f"{self.db_host}:{self.db_port}/{self.db_name}"
            )
            logger.info("✓ PostgreSQL connection string built successfully")
            return connection_string
        
        except Exception as e:
            logger.error(f"✗ Error building connection string: {e}")
            raise DatabaseConnectorError(f"Connection string build failed: {str(e)}")
    
    
    def get_connection_string(self):
        """Get the full database connection string"""
        return self.connection_string
    
    
    def test_connection(self):
        """Test database connection"""
        try:
            try:
                import psycopg2
            except ImportError:
                logger.error("✗ psycopg2 not installed. Install with: pip install psycopg2-binary")
                return {
                    'connected': False,
                    'timestamp': datetime.utcnow().isoformat(),
                    'error': 'psycopg2 library not installed',
                    'schema': self.db_schema
                }
            
            try:
                conn_params = {
                    'host': self.db_host,
                    'port': int(self.db_port),
                    'database': self.db_name,
                    'user': self.db_user,
                    'password': self.db_password,
                    'connect_timeout': 10,
                    'options': f'-c search_path={self.db_schema}'
                }
                
                if self.db_sslmode != 'disable':
                    conn_params['sslmode'] = self.db_sslmode
                
                logger.info("\n📍 Attempting to connect to database...")
                conn = psycopg2.connect(**conn_params)
                cursor = conn.cursor()
                
                logger.info("✓ Connection established successfully!")
                
                # Test: Get database version
                cursor.execute('SELECT version();')
                version = cursor.fetchone()[0]
                logger.info(f"✓ PostgreSQL version: {version[:50]}...")
                
                # Test: Get current schema
                cursor.execute("SELECT current_schema();")
                current_schema = cursor.fetchone()[0]
                logger.info(f"✓ Current schema in use: {current_schema}")
                
                if current_schema == self.db_schema:
                    logger.info(f"✓ Schema matches configured schema: {self.db_schema}")
                else:
                    logger.warning(f"⚠ Schema mismatch! Expected '{self.db_schema}', got '{current_schema}'")
                
                # Test: Get database info
                cursor.execute("""
                    SELECT 
                        current_database(),
                        current_user,
                        pg_database_size(current_database())
                """)
                db_info = cursor.fetchone()
                logger.info(f"✓ Database: {db_info[0]}")
                logger.info(f"✓ User: {db_info[1]}")
                logger.info(f"✓ Database size: {db_info[2]} bytes")
                
                cursor.close()
                conn.close()
                
                logger.info("✓ Connection test PASSED ✅")
                self.is_connected = True
                
                return {
                    'connected': True,
                    'timestamp': datetime.utcnow().isoformat(),
                    'version': version,
                    'host': self.db_host,
                    'port': self.db_port,
                    'database': self.db_name,
                    'user': self.db_user,
                    'schema': current_schema,
                    'configured_schema': self.db_schema,
                    'db_size': db_info[2]
                }
            
            except Exception as pg_err:
                logger.error(f"✗ PostgreSQL connection test FAILED: {pg_err}")
                self.is_connected = False
                
                return {
                    'connected': False,
                    'timestamp': datetime.utcnow().isoformat(),
                    'error': str(pg_err),
                    'host': self.db_host,
                    'port': self.db_port,
                    'database': self.db_name,
                    'schema': self.db_schema
                }
        
        except Exception as e:
            logger.error(f"✗ Unexpected error in connection test: {e}")
            
            return {
                'connected': False,
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e),
                'schema': self.db_schema
            }
    
    
    def health_check(self):
        """Perform comprehensive database health check"""
        try:
            import time
            import psycopg2
            
            start_time = time.time()
            
            try:
                conn_params = {
                    'host': self.db_host,
                    'port': int(self.db_port),
                    'database': self.db_name,
                    'user': self.db_user,
                    'password': self.db_password,
                    'connect_timeout': 5,
                    'options': f'-c search_path={self.db_schema}'
                }
                
                if self.db_sslmode != 'disable':
                    conn_params['sslmode'] = self.db_sslmode
                
                conn = psycopg2.connect(**conn_params)
                cursor = conn.cursor()
                
                # Simple connectivity test
                cursor.execute('SELECT 1;')
                cursor.fetchone()
                logger.info("✓ Health check: Connection OK")
                
                # Verify schema exists
                cursor.execute("""
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.schemata 
                        WHERE schema_name = %s
                    );
                """, (self.db_schema,))
                
                schema_exists = cursor.fetchone()[0]
                
                if not schema_exists:
                    logger.warning(f"⚠ Schema '{self.db_schema}' does not exist in database!")
                else:
                    logger.info(f"✓ Schema '{self.db_schema}' exists")
                
                # Get database stats
                cursor.execute("""
                    SELECT 
                        datname,
                        pg_database_size(datname) as size,
                        numbackends
                    FROM pg_stat_database
                    WHERE datname = %s;
                """, (self.db_name,))
                
                db_stats = cursor.fetchone()
                
                cursor.close()
                conn.close()
                
                response_time = (time.time() - start_time) * 1000
                status = 'healthy' if response_time < 1000 and schema_exists else 'degraded'
                
                logger.info(f"✓ Response time: {response_time:.2f}ms")
                
                return {
                    'status': status,
                    'connected': True,
                    'response_time': response_time,
                    'timestamp': datetime.utcnow().isoformat(),
                    'details': {
                        'host': self.db_host,
                        'database': self.db_name,
                        'schema': self.db_schema,
                        'schema_exists': schema_exists,
                        'port': self.db_port,
                        'db_size': db_stats[1] if db_stats else 'N/A',
                        'active_connections': db_stats[2] if db_stats else 'N/A'
                    }
                }
            
            except Exception as pg_err:
                logger.error(f"✗ Health check failed: {pg_err}")
                response_time = (time.time() - start_time) * 1000
                
                return {
                    'status': 'unhealthy',
                    'connected': False,
                    'response_time': response_time,
                    'timestamp': datetime.utcnow().isoformat(),
                    'error': str(pg_err),
                    'details': {
                        'host': self.db_host,
                        'database': self.db_name,
                        'schema': self.db_schema
                    }
                }
        
        except Exception as e:
            logger.error(f"✗ Unexpected error in health check: {e}")
            
            return {
                'status': 'unhealthy',
                'connected': False,
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e),
                'schema': self.db_schema
            }
    
    
    def get_db_info(self):
        """Get detailed database information"""
        try:
            info = {
                'type': self.db_type,
                'host': self.db_host,
                'port': self.db_port,
                'database': self.db_name,
                'user': self.db_user,
                'schema': self.db_schema,
                'ssl_mode': self.db_sslmode,
                'pool_size': self.pool_size,
                'connected': self.is_connected,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            return info
        
        except Exception as e:
            logger.error(f"✗ Error getting database info: {e}")
            return {
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }


# ============================================================================
# TEST FUNCTIONS
# ============================================================================

def print_section(title):
    """Print a formatted section header"""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def test_all():
    """Run all database tests"""
    
    print_section("1. INITIALIZING DATABASE CONNECTOR")
    
    try:
        connector = DatabaseConnector()
    except Exception as e:
        logger.error(f"Failed to initialize connector: {e}")
        print("\n❌ FAILED TO INITIALIZE CONNECTOR")
        return False
    
    # Test 1: Connection String
    print_section("2. CONNECTION STRING")
    conn_str = connector.get_connection_string()
    logger.info(f"Connection string: {conn_str}")
    print(f"\n{conn_str}")
    
    # Test 2: Database Info
    print_section("3. DATABASE INFORMATION")
    db_info = connector.get_db_info()
    for key, value in db_info.items():
        logger.info(f"{key}: {value}")
        print(f"  {key}: {value}")
    
    # Test 3: Connection Test
    print_section("4. CONNECTION TEST")
    result = connector.test_connection()
    
    if result['connected']:
        print("\n✅ CONNECTION TEST PASSED\n")
        for key, value in result.items():
            if key != 'connected':
                print(f"  {key}: {value}")
    else:
        print("\n❌ CONNECTION TEST FAILED\n")
        print(f"  Error: {result.get('error', 'Unknown error')}")
        return False
    
    # Test 4: Health Check
    print_section("5. HEALTH CHECK")
    health = connector.health_check()
    
    print(f"\nStatus: {health['status']}")
    print(f"Connected: {health['connected']}")
    print(f"Response time: {health['response_time']:.2f}ms")
    
    if health.get('details'):
        print("\nDetails:")
        for key, value in health['details'].items():
            print(f"  {key}: {value}")
    
    if health.get('error'):
        print(f"\nError: {health['error']}")
    
    # Summary
    print_section("6. TEST SUMMARY")
    
    if result['connected'] and health['status'] == 'healthy':
        print("\n✅ ALL TESTS PASSED!")
        print("\n📋 Database Connection Details:")
        print(f"   Host: {connector.db_host}:{connector.db_port}")
        print(f"   Database: {connector.db_name}")
        print(f"   Schema: {connector.db_schema}")
        print(f"   User: {connector.db_user}")
        return True
    else:
        print("\n⚠️  SOME TESTS FAILED")
        if not result['connected']:
            print("   - Connection test failed")
        if health['status'] != 'healthy':
            print("   - Health check failed")
        return False


if __name__ == '__main__':
    try:
        success = test_all()
        
        print_section("TEST COMPLETED")
        print(f"\nLog file: db_test.log")
        print(f"Timestamp: {datetime.now().isoformat()}\n")
        
        sys.exit(0 if success else 1)
    
    except KeyboardInterrupt:
        logger.info("\n\nTest interrupted by user")
        print("\n\n❌ Test interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)
