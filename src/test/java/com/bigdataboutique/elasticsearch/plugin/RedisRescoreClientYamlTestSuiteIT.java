package com.bigdataboutique.elasticsearch.plugin;

import java.security.AccessController;
import java.security.PrivilegedAction;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.After;
import org.junit.Before;
import redis.clients.jedis.Jedis;

public class RedisRescoreClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {
    final Jedis jedis = new Jedis("localhost");

    @Before
    public void startRedis() {
        AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {
            jedis.set("foo", "5.0");
            jedis.set("bar", "3.0");
    
            jedis.set("mystore-foo", "4.0");
            jedis.set("mystore-bar", "6.0");

            return true;
        });
    }

    @After
    public void closeRedisConnection() {
        jedis.close();
    }

    public RedisRescoreClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }
}