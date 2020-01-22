package com.bigdataboutique.elasticsearch.plugin;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.List;

import static java.util.Collections.singletonList;

public class RedisRescorePlugin extends Plugin implements SearchPlugin {
    @Override
    public List<RescorerSpec<?>> getRescorers() {
        return singletonList(
                new RescorerSpec<>(RedisRescoreBuilder.NAME, RedisRescoreBuilder::new, RedisRescoreBuilder::fromXContent));
    }
}