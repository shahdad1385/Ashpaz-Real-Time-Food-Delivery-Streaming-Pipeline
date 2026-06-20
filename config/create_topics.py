from confluent_kafka.admin import AdminClient, NewTopic

def create_ashpaz_topics():
    # Connect to the local Kafka broker
    admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
    
    # Define the exact topics required by Q2 of the PDF assignment
    topic_list = [
        NewTopic("ashpaz.order", num_partitions=1, replication_factor=1),
        NewTopic("ashpaz.valid", num_partitions=1, replication_factor=1),
        NewTopic("ashpaz.error_log", num_partitions=1, replication_factor=1)
    ]
    
    # Execute creation
    futures = admin_client.create_topics(topic_list)
    
    for topic, future in futures.items():
        try:
            future.result()  # The result itself is None if successful
            print(f"🎉 Topic '{topic}' created successfully!")
        except Exception as e:
            print(f"⚠️ Topic '{topic}' status: {e}")

if __name__ == "__main__":
    create_ashpaz_topics()
