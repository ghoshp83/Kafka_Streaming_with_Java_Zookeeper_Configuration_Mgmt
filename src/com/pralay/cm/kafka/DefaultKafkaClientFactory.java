package com.pralay.cm.kafka;

import com.pralay.cm.ConfigurationException;
import com.pralay.cm.ServiceAddressProvider;
import com.pralay.cm.APPConfigurations;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Properties;

public class DefaultKafkaClientFactory implements KafkaClientFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKafkaClientFactory.class);

    private static final String KEY_TARGET = "key";
    private static final String VALUE_TARGET = "value";
    private static final String GROUP_ID = "group.id";

    private final Configuration configuration;
    private final ServiceAddressProvider serviceAddressProvider;

    @Inject
    public DefaultKafkaClientFactory(@Named(ServiceAddressProvider.KAFKA_SERVICE_QUALIFIER) final ServiceAddressProvider serviceAddressProvider, final Configuration configuration) {
        this.serviceAddressProvider = serviceAddressProvider;
        this.configuration = configuration;
    }

    @Override
    public <K, V> Producer<K, V> getProducer(Class<K> k, Class<V> v) {
        Properties props = getProperties("kafka.producer.config");

        Serializer<K> producerKeySerializer = getProducerSerializer(KEY_TARGET, k);
        Serializer<V> producerValueSerializer = getProducerSerializer(VALUE_TARGET, v);
        LOGGER.info("kafka producer created\nkey.serializer = {}\nvalue.serializer = {}\nconfig = {}",
                producerKeySerializer.getClass().getName(),
                producerValueSerializer.getClass().getName(),
                Maps.fromProperties(props));
        return new KafkaProducer<>(props, producerKeySerializer, producerValueSerializer);
    }

    public <K, V> Consumer<K, V> getConsumer(Class<K> k, Class<V> v, String groupId) {
        Properties props = getProperties("kafka.consumer.config");
        if (!StringUtils.isBlank(groupId) && !props.containsKey(GROUP_ID)) {
            props.setProperty(GROUP_ID, groupId);
        }

        Deserializer<K> consumerKeyDeserializer = getConsumerDeserializer(KEY_TARGET, k);
        Deserializer<V> consumerValueDeserializer = getConsumerDeserializer(VALUE_TARGET, v);
        LOGGER.info("kafka consumer created\nkey.deserializer = {}\nvalue.deserializer = {}\nconfig = {}",
                consumerKeyDeserializer.getClass().getName(),
                consumerValueDeserializer.getClass().getName(),
                Maps.fromProperties(props));
        return new KafkaConsumer<>(props, consumerKeyDeserializer, consumerValueDeserializer);
    }

    @Override
    public <K, V> Consumer<K, V> getConsumer(Class<K> k, Class<V> v) {
        return getConsumer(k, v, null);
    }

    private Properties getProperties(final String prefix) {
        Properties props = new Properties();
        props.put("bootstrap.servers", getBootstrapServers());
        props.putAll(Maps.fromProperties(APPConfigurations.getProperties(configuration.subset(prefix))));
        return props;
    }

    private String getBootstrapServers() {
        return Joiner.on(',').join(serviceAddressProvider.getAddresses());
    }

    private <C,K> C getConverter(String target, Class<K> k, Class<C> c, Class<? extends C> defaultConverterClass) {
        try {
            String configKey = "kafka." + target + "." + k.getName();
            return Class.forName(
                    configuration.getString(
                            configKey,
                            defaultConverterClass.getName()
                    )
            ).asSubclass(c).newInstance();
        } catch (InstantiationException | IllegalAccessException  | ClassNotFoundException e) {
            throw new ConfigurationException(e);
        }
    }


    private <K> Serializer<K> getProducerSerializer(String keyOrValue, Class<K> k) {
        return getConverter("producer." + keyOrValue + ".serializer", k, Serializer.class, StringSerializer.class);
    }

    private <K> Deserializer<K> getConsumerDeserializer(String keyOrValue, Class<K> k) {
        return getConverter("consumer." + keyOrValue + ".deserializer", k, Deserializer.class, StringDeserializer.class);
    }
}
