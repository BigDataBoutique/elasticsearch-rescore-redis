package com.bigdataboutique.elasticsearch.plugin;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class RedisRescoreBuilderTests extends AbstractWireSerializingTestCase<RedisRescoreBuilder> {
    @Override
    protected RedisRescoreBuilder createTestInstance() {
        String factorField = randomBoolean() ? null : randomAlphaOfLength(5);
        try {
            return new RedisRescoreBuilder("prefix-", factorField,"MULTIPLY",
                    null,"MULTIPLY",null, null, null).windowSize(between(0, Integer.MAX_VALUE));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    protected Writeable.Reader<RedisRescoreBuilder> instanceReader() {
        return RedisRescoreBuilder::new;
    }

    @Override
    protected RedisRescoreBuilder mutateInstance(RedisRescoreBuilder instance) throws IOException {
        return new RedisRescoreBuilder(instance.keyField(), instance.keyPrefix(), instance.scoreOperator(),
                instance.keyPrefixes(), instance.boostOperator(), instance.boostWeight(), instance.scoreWeights()
                , instance.scoreFunctions());
    }


    public void testRewrite() throws IOException {
        RedisRescoreBuilder builder = createTestInstance();
        assertSame(builder, builder.rewrite(null));
    }

//    public void testRescore() throws IOException {
//        // Always use a factor > 1 so rescored fields are sorted in front of the unrescored fields.
//        float factor = (float) randomDoubleBetween(1.0d, Float.MAX_VALUE, false);
//        RedisRescoreBuilder builder = new RedisRescoreBuilder("productId", "mystore-").windowSize(2);
//        RescoreContext context = builder.buildContext(null);
//        TopDocs docs = new TopDocs(new TotalHits(10, TotalHits.Relation.EQUAL_TO), new ScoreDoc[3]);
//        docs.scoreDocs[0] = new ScoreDoc(0, 1.0f);
//        docs.scoreDocs[1] = new ScoreDoc(1, 1.0f);
//        docs.scoreDocs[2] = new ScoreDoc(2, 1.0f);
//        context.rescorer().rescore(docs, null, context);
//        assertEquals(factor, docs.scoreDocs[0].score, 0.0f);
//        assertEquals(factor, docs.scoreDocs[1].score, 0.0f);
//        assertEquals(1.0f, docs.scoreDocs[2].score, 0.0f);
//    }
}
