package io.github.bulbaattacks.KafkaAppender.util;

import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import java.util.Properties;

@Plugin(name = "Properties", category = "Core")
public class PropertiesElement {
    private final Properties properties = new Properties();

    public Properties getProperties() {
        return properties;
    }

    @PluginFactory
    public static PropertiesElement createProperties(
            @PluginElement("Property") PropertyElement[] propertyElements) {
        PropertiesElement element = new PropertiesElement();
        if (propertyElements != null) {
            for (PropertyElement p : propertyElements) {
                element.properties.setProperty(p.getName(), p.getValue());
            }
        }
        return element;
    }
}

