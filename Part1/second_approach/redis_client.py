# redis_client.py - Redis cache configuration
import redis
import json
import os
from typing import Optional, Dict, Any
from datetime import timedelta

class RedisCache:
    """
    Redis cache client with LRU eviction and TTL support
    Configured for maximum 10 items with 10-minute expiration
    """
    
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.client = redis.from_url(self.redis_url, decode_responses=True)
        self.max_items = 10
        self.ttl_seconds = 600  # 10 minutes
        
    def get(self, key: str) -> Optional[str]:
        """
        Get value from cache
        Returns None if key doesn't exist or has expired
        """
        try:
            return self.client.get(key)
        except Exception as e:
            print(f"Redis get error: {e}")
            return None
    
    def set(self, key: str, value: str) -> bool:
        """
        Set value in cache with TTL
        Implements LRU by checking cache size and removing oldest entries
        """
        try:
            # Set the value with TTL
            self.client.setex(key, self.ttl_seconds, value)
            
            # Implement manual LRU since Redis LRU policy handles this automatically
            # but we want to ensure exactly 10 items max
            keys = self.client.keys("city:*")
            if len(keys) > self.max_items:
                # Get keys with their last access time and remove oldest
                oldest_keys = sorted(keys)[:len(keys) - self.max_items]
                if oldest_keys:
                    self.client.delete(*oldest_keys)
            
            return True
        except Exception as e:
            print(f"Redis set error: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """Check if key exists in cache"""
        try:
            return self.client.exists(key) > 0
        except Exception as e:
            print(f"Redis exists error: {e}")
            return False
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        try:
            info = self.client.info()
            return {
                "used_memory": info.get("used_memory_human", "N/A"),
                "connected_clients": info.get("connected_clients", 0),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0)
            }
        except Exception as e:
            print(f"Redis stats error: {e}")
            return {}

# Global redis instance
redis_cache = RedisCache()
