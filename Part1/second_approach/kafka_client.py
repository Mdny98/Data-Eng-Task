from kafka import KafkaProducer
import json
import os
from datetime import datetime
from typing import Dict, Any

class KafkaLogger:
    """
    Kafka producer for logging API requests
    Sends structured log messages to the 'city-logs' topic
    """
    
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = "city-logs"
        
        # Configure producer with JSON serialization
        self.producer = KafkaProducer(
            bootstrap_servers=[self.bootstrap_servers],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None,
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,
            retry_backoff_ms=1000
        )
    
    def log_request(self, city: str, response_time_ms: float, cache_hit: bool, cache_hit_percentage: float):
        """
        Log API request details to Kafka
        
        Args:
            city: City name that was requested
            response_time_ms: Response time in milliseconds
            cache_hit: Whether the request was served from cache
            cache_hit_percentage: Current cache hit percentage
        """
        log_message = {
            "timestamp": datetime.utcnow().isoformat(),
            "city": city,
            "response_time_ms": response_time_ms,
            "cache_hit": cache_hit,
            "cache_hit_percentage": cache_hit_percentage,
            "event_type": "city_lookup"
        }
        
        try:
            # Send message with city as key for partitioning
            future = self.producer.send(
                self.topic,
                key=city,
                value=log_message
            )
            
            # Optional: wait for message to be sent (uncomment for synchronous sending)
            # future.get(timeout=10)
            
        except Exception as e:
            print(f"Failed to send log to Kafka: {e}")
    
    def close(self):
        """Close the producer and flush any pending messages"""
        self.producer.flush()
        self.producer.close()

# Global kafka logger instance
kafka_logger = KafkaLogger()
