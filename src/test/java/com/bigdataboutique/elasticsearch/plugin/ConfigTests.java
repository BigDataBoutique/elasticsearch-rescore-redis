package com.bigdataboutique.elasticsearch.plugin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static com.bigdataboutique.elasticsearch.plugin.Config.REDIS_URL;

/**
 * {@link ConfigTests} is a unit test class for {@link Config}.
 * <p>
 * It's a JUnit test class that extends {@link ESTestCase} which provides useful methods for testing.
 * <p>
 * The tests can be executed in the IDE or using the command: ./gradlew test
 */
public class ConfigTests extends ESTestCase {

    public void testValidatedSetting() {
        final String expected = "http://localhost:6379";
        final String actual = REDIS_URL.get(Settings.builder().put(REDIS_URL.getKey(), expected).build());
        assertEquals(expected, actual);

        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
            REDIS_URL.get(Settings.builder().put(REDIS_URL.getKey(), "not an URI").build()));
        assertEquals("Setting is not a valid URI", exception.getMessage());
    }
}
