package com.bigdataboutique.elasticsearch.plugin;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.List;
import static java.util.Collections.singletonList;
import java.nio.file.Path;

public class RedisRescorePlugin extends Plugin implements SearchPlugin {
    private final Config config;

    public RedisRescorePlugin(final Settings settings, final Path configPath) {
        this.config = new Config(settings);
        RedisRescoreBuilder.setJedis(new Jedis(config.getRedisUrl()));
    }

    /**
     * @return the plugin's custom settings
     */
    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(Config.REDIS_URL);
    }

    @Override
    public Settings additionalSettings() {
        final Settings.Builder builder = Settings.builder();

        // Exposes REDIS_URL as a node setting
        builder.put(Config.REDIS_URL.getKey(), config.getRedisUrl());

        return builder.build();
    }

    @Override
    public List<RescorerSpec<?>> getRescorers() {
        return singletonList(
                new RescorerSpec<>(RedisRescoreBuilder.NAME, RedisRescoreBuilder::new, RedisRescoreBuilder::fromXContent));
    }
}