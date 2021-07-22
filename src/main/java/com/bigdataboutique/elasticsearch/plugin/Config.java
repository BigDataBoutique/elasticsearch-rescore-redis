
package com.bigdataboutique.elasticsearch.plugin;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import java.net.URI;
/**
 * {@link Config} contains the settings values and their static declarations.
 */
public class Config {
    static final Setting<String> REDIS_URL = new Setting<String>("redisRescore.redisUrl", "localhost", v -> {
        try {
            new URI(v);
            return v;
        } catch (Exception e) {
            throw new IllegalArgumentException("Setting is not a valid URI");
        }
    }, Property.NodeScope, Property.Dynamic);

    private final String redisUrl;

    public Config(final Settings settings) {
        this.redisUrl = REDIS_URL.get(settings);
    }

    public String getRedisUrl() {
        return redisUrl;
    }

}
