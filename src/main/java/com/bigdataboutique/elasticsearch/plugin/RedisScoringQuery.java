package com.bigdataboutique.elasticsearch.plugin;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

import java.io.IOException;

/**
 * A query that wraps another query, and uses a DoubleValuesSource to
 * replace or modify the wrapped query's score
 *
 * If the DoubleValuesSource doesn't return a value for a particular document,
 * then that document will be given a score of 0.
 */
public class RedisScoringQuery extends Query {
    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        // TODO
        return super.rewrite(reader);
    }

    @Override
    public String toString(String field) {
        return "redisscoringquery:" + field;
    }

    @Override
    public boolean equals(Object obj) {
        // TODO
        return false;
    }

    @Override
    public int hashCode() {
        //return Objects.hash(wrapped, vectorSupplier);
        return 0;
    }
}
