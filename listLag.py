from kafka import KafkaAdminClient

BOOTSTRAP_SERVERS = "localhost:9092"

def main():

  client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

  for group in client.list_consumer_groups():
    if group[1] == 'consumer':
      print("group[0] : {}, group[1] : {}".format(group[0],group[1]))
      consumer = KafkaConsumer(
                                   bootstrap_servers=BOOTSTRAP_SERVERS,
                                   group_id=group[0],
                                   enable_auto_commit=False,
                                   api_version=(0,10)
                                 )
      list_members_in_groups =  client.list_consumer_group_offsets(group[0])
      for (topic,partition) in list_members_in_groups:

        print("group: {} topic:{} partition: {}".format(group[0], topic, partition));

        consumer.topics()

        tp = TopicPartition(topic, partition)
        consumer.assign([tp])
        committed = consumer.committed(tp)
        consumer.seek_to_end(tp)
        last_offset = consumer.position(tp)
        if last_offset != None and committed != None:
          lag = last_offset - committed
          print("group: {} topic:{} partition: {} lag: {}".format(group[0], topic, partition, lag))
      consumer.close(autocommit=False)
