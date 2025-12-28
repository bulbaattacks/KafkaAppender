package io.github.bulbaattacks.KafkaAppender.core;

import io.github.bulbaattacks.KafkaAppender.util.PropertiesElement;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import java.io.Serializable;
import java.util.Properties;

@Plugin(name = "KafkaAppender", category = "Core", elementType = Appender.ELEMENT_TYPE, printObject = true)
public class KafkaAppender extends AbstractAppender {
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaAppender(String name,
                         Filter filter,
                         Layout<? extends Serializable> layout,
                         boolean ignoreExceptions,
                         String topic,
                         Properties props) {
        super(name, filter, layout, ignoreExceptions);
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
    }

    @Override
    public void append(LogEvent event) {
        try {
            String message = new String(getLayout().toByteArray(event));
            producer.send(new ProducerRecord<>(topic, message));
            producer.flush();
        } catch (Exception e) {
            LOGGER.error("Ошибка при отправке лога в Kafka", e);
            if (!ignoreExceptions()) {
                throw new RuntimeException("Ошибка при отправке лога в Kafka", e);
            }
        }
    }

    @Override
    public void stop() {
        super.stop();
        producer.close();
    }

    @PluginFactory
    public static KafkaAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("topic") String topic,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") Filter filter,
            @PluginAttribute(value = "ignoreExceptions", defaultBoolean = true) boolean ignoreExceptions,
            @PluginElement("Properties") PropertiesElement propertiesElement) {

        if (name == null) {
            LOGGER.error("Не указано имя для KafkaAppender");
            return null;
        }
        if (topic == null) {
            LOGGER.error("Не указан топик для KafkaAppender");
            return null;
        }

        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        if (propertiesElement != null) {
            props.putAll(propertiesElement.getProperties());
        }
        return new KafkaAppender(name, filter, layout, ignoreExceptions, topic, props);
    }
}
