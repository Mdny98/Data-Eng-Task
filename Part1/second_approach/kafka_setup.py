

# kafka_setup.py - Script to create Kafka topics
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import time

def create_kafka_topic():
    """
    Create Kafka topic for city logs
    """
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092",
        client_id='city_api_admin'
    )
    
    topic = NewTopic(
        name="city-logs",
        num_partitions=3,
        replication_factor=1
    )
    
    try:
        admin_client.create_topics([topic], validate_only=False)
        print("Topic 'city-logs' created successfully")
    except TopicAlreadyExistsError:
        print("Topic 'city-logs' already exists")
    except Exception as e:
        print(f"Error creating topic: {e}")
    finally:
        admin_client.close()

def wait_for_kafka():
    """Wait for Kafka to be ready"""
    import socket
    
    print("Waiting for Kafka to be ready...")
    for _ in range(30):  # Wait up to 30 seconds
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('localhost', 9092))
            sock.close()
            if result == 0:
                print("Kafka is ready!")
                return True
        except:
            pass
        time.sleep(1)
    
    print("Kafka is not ready after 30 seconds")
    return False

if __name__ == "__main__":
    if wait_for_kafka():
        create_kafka_topic()
    else:
        print("Failed to connect to Kafka")

---