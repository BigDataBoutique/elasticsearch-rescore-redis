package com.bigdataboutique.elasticsearch.plugin;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetDVBytesAtomicFieldData;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.search.rescore.RescorerBuilder;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class RedisRescoreBuilder extends RescorerBuilder<RedisRescoreBuilder> {
    public static final String NAME = "redis";

    private final String keyField;
    private final String keyPrefix;

    private static Jedis jedis;
    public static void setJedis(Jedis j) {
        jedis = j;
    }

    public RedisRescoreBuilder(final String keyField, @Nullable String keyPrefix) {
        this.keyField = keyField;
        this.keyPrefix = keyPrefix;
    }

    public RedisRescoreBuilder(StreamInput in) throws IOException {
        super(in);
        keyField = in.readString();
        keyPrefix = in.readOptionalString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(keyField);
        out.writeOptionalString(keyPrefix);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public RescorerBuilder<RedisRescoreBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
        return this;
    }

    private static final ParseField KEY_FIELD = new ParseField("key_field");
    private static final ParseField KEY_PREFIX = new ParseField("key_prefix");
    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(KEY_FIELD.getPreferredName(), keyField);
        if (keyPrefix != null) {
            builder.field(KEY_PREFIX.getPreferredName(), keyPrefix);
        }
    }

    private static final ConstructingObjectParser<RedisRescoreBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> new RedisRescoreBuilder((String) args[0], (String) args[1]));
    static {
        PARSER.declareString(constructorArg(), KEY_FIELD);
        PARSER.declareString(optionalConstructorArg(), KEY_PREFIX);
    }
    public static RedisRescoreBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public RescoreContext innerBuildContext(int windowSize, QueryShardContext context) throws IOException {
        IndexFieldData<?> keyField =
                this.keyField == null ? null : context.getForField(context.fieldMapper(this.keyField));
        return new RedisRescoreContext(windowSize, keyPrefix, keyField);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        RedisRescoreBuilder other = (RedisRescoreBuilder) obj;
        return keyField.equals(other.keyField)
                && Objects.equals(keyPrefix, other.keyPrefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyField, keyPrefix);
    }

    String keyField() {
        return keyField;
    }

    @Nullable
    String keyPrefix() {
        return keyPrefix;
    }

    private static class RedisRescoreContext extends RescoreContext {
        private final String keyPrefix;
        @Nullable
        private final IndexFieldData<?> keyField;

        RedisRescoreContext(int windowSize, String keyPrefix, @Nullable IndexFieldData<?> keyField) {
            super(windowSize, RedisRescorer.INSTANCE);
            this.keyPrefix = keyPrefix;
            this.keyField = keyField;
        }
    }

    private static class RedisRescorer implements Rescorer {

        private static final RedisRescorer INSTANCE = new RedisRescorer();

        private static String getTermFromFieldData(int topLevelDocId, AtomicFieldData fd,
                LeafReaderContext leaf, String fieldName) throws IOException {
            String term = null;
            if (fd instanceof SortedSetDVBytesAtomicFieldData) {
                final SortedSetDocValues data = ((SortedSetDVBytesAtomicFieldData) fd).getOrdinalsValues();
                if (data != null) {
                    if (data.advanceExact(topLevelDocId - leaf.docBase)) {
                        // document does have data for the field
                        term = data.lookupOrd(data.nextOrd()).utf8ToString();
                    }
                }
            } else if (fd instanceof AtomicNumericFieldData) {
                final SortedNumericDocValues data = ((AtomicNumericFieldData) fd).getLongValues();
                if (data != null) {
                    if (!data.advanceExact(topLevelDocId - leaf.docBase)) {
                        throw new IllegalArgumentException("document [" + topLevelDocId
                                + "] does not have the field [" + fieldName + "]");
                    }
                    if (data.docValueCount() > 1) {
                        throw new IllegalArgumentException("document [" + topLevelDocId
                                + "] has more than one value for [" + fieldName + "]");
                    }
                    term = String.valueOf(data.nextValue());
                }
            }
            return term;
        }

        @Override
        public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {
            RedisRescoreContext context = (RedisRescoreContext) rescoreContext;

            if (context.keyField != null) {
                /*
                 * Since this example looks up a single field value it should
                 * access them in docId order because that is the order in
                 * which they are stored on disk and we want reads to be
                 * forwards and close together if possible.
                 *
                 * If accessing multiple fields we'd be better off accessing
                 * them in (reader, field, docId) order because that is the
                 * order they are on disk.
                 */
                final Iterator<LeafReaderContext> leaves = searcher.getIndexReader().leaves().iterator();
                LeafReaderContext leaf = null;

                final int end = Math.min(topDocs.scoreDocs.length, rescoreContext.getWindowSize());
                int endDoc = 0;
                for (int i = 0; i < end; i++) {
                    final int topLevelDocId = topDocs.scoreDocs[i].doc;
                    if (topLevelDocId >= endDoc) {
                        do {
                            leaf = leaves.next();
                            endDoc = leaf.docBase + leaf.reader().maxDoc();
                        } while (topDocs.scoreDocs[i].doc >= endDoc);

                        final AtomicFieldData fd = context.keyField.load(leaf);
                        final String term = getTermFromFieldData(topLevelDocId, fd, leaf, context.keyField.getFieldName());
                        if (term != null) {
                            topDocs.scoreDocs[i].score *= getScoreFactor(term, context.keyPrefix);
                        }
                    }
                }
            }

            // Sort by score descending, then docID ascending, just like lucene's QueryRescorer
            Arrays.sort(topDocs.scoreDocs, (a, b) -> {
                if (a.score > b.score) {
                    return -1;
                }
                if (a.score < b.score) {
                    return 1;
                }
                // Safe because doc ids >= 0
                return a.doc - b.doc;
            });
            return topDocs;
        }

        private static float getScoreFactor(final String key, @Nullable final String keyPrefix) {
            assert key != null;

            return AccessController.doPrivileged((PrivilegedAction<Float>) () -> {
                final String factor;
                if (keyPrefix == null) {
                    factor = jedis.get(key);
                } else {
                    factor = jedis.get(keyPrefix + key);
                }

                if (factor == null) {
                    return 1.0f;
                }

                try {
                    return Float.parseFloat(factor);
                } catch (NumberFormatException ignored_e) {
                    return 1.0f;
                }
            });
        }

        @Override
        public Explanation explain(int topLevelDocId, IndexSearcher searcher, RescoreContext rescoreContext,
                                   Explanation sourceExplanation) throws IOException {
            final RedisRescoreContext context = (RedisRescoreContext) rescoreContext;
            final Iterator<LeafReaderContext> leaves = searcher.getIndexReader().leaves().iterator();
            LeafReaderContext leaf = null;
            int endDoc = 0;
            do {
                leaf = leaves.next();
                endDoc = leaf.docBase + leaf.reader().maxDoc();
            } while (topLevelDocId >= endDoc);

            AtomicFieldData fd = context.keyField.load(leaf);
            String fieldName = context.keyField.getFieldName();
            String term = getTermFromFieldData(topLevelDocId, fd, leaf, fieldName);
            if (term != null) {
                float score = getScoreFactor(term, context.keyPrefix);
                return Explanation.match(score, fieldName, singletonList(sourceExplanation));
            } else {
                return Explanation.noMatch(fieldName, sourceExplanation);
            }
        }
    }
}
