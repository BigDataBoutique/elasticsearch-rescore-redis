package com.bigdataboutique.elasticsearch.plugin;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.security.AccessController;
import java.security.PrivilegedAction;

public class RedisRescoreClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    @SuppressWarnings("rawtypes")
    public static GenericContainer redis;

    public RedisRescoreClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            redis = new GenericContainer<>("redis:3.2.0").withNetworkMode("host");
            redis.start();
            return null;
        });
    }

    @AfterClass
    public static void afterClass() throws Exception {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            redis.stop();
            return null;
        });
    }
}