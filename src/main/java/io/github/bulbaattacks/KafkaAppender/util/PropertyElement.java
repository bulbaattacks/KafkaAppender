package io.github.bulbaattacks.KafkaAppender.util;

import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

@Plugin(name = "Property", category = "Core")
public class PropertyElement {
    private final String name;
    private final String value;

    private PropertyElement(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    @PluginFactory
    public static PropertyElement createProperty(
            @PluginAttribute("name") String name,
            @PluginAttribute("value") String value) {
        return new PropertyElement(name, value);
    }
}

