package com.pralay.cm.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

public interface KafkaClientFactory {
    <K,V> Producer<K, V> getProducer(Class<K> k, Class<V> v);

    <K,V> Consumer<K, V> getConsumer(Class<K> k, Class<V> v);

    <K, V> Consumer<K, V> getConsumer(Class<K> k, Class<V> v, String groupId);
}
