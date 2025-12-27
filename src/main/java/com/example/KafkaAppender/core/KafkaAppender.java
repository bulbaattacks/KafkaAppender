package com.example.KafkaAppender.core;

import com.example.KafkaAppender.kafka.KafkaConfig;
import com.example.KafkaAppender.kafka.KafkaProducerService;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;
import java.util.Properties;

@Plugin(name = "KafkaAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class KafkaAppender extends AbstractAppender {
    private KafkaProducerService producerService;
    private final String topic;
    private final Properties properties;

    public KafkaAppender(String name, Layout<? extends Serializable> layout,
                         boolean ignoreExceptions, String topic, Properties props) {
        super(name, null, layout, ignoreExceptions);
        this.topic = topic;
        this.properties = props;
    }

    @Override
    public void append(LogEvent event) {
        String msg = new String(getLayout().toByteArray(event));
        producerService.send(msg);
    }

    @Override
    public void start() {
        super.start();
        producerService = new KafkaProducerService(topic, properties);
    }

    @Override
    public void stop() {
        super.stop();
        if (producerService != null) {
            producerService.close();
        }
    }

    @PluginFactory
    public static KafkaAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("topic") String topic,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Properties") Property[] properties,
            @PluginAttribute(value = "ignoreExceptions", defaultBoolean = true) boolean ignoreExceptions) {

        if (layout == null) {
            layout = PatternLayout.newBuilder()
                    .withPattern("%d{ISO8601} %-5p %c - %m%n")
                    .build();
        }

        Properties props = KafkaConfig.buildDefaultProperties();
        if (properties != null) {
            for (Property p : properties) {
                props.setProperty(p.getName(), p.getValue());
            }
        }
        return new KafkaAppender(name, layout, ignoreExceptions, topic, props);
    }
}
