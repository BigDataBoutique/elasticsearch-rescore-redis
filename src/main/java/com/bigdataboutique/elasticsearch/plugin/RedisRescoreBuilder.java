package com.bigdataboutique.elasticsearch.plugin;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
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
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.plain.SortedSetDVBytesAtomicFieldData;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.search.rescore.RescorerBuilder;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class RedisRescoreBuilder extends RescorerBuilder<RedisRescoreBuilder> {
    public static final String NAME = "redis";

    private final String keyField;
    private final String keyPrefix;

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

        private static final Jedis jedis = new Jedis("localhost"); // TODO host from settings

        @Override
        public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {
            RedisRescoreContext context = (RedisRescoreContext) rescoreContext;

            final int end = Math.min(topDocs.scoreDocs.length, rescoreContext.getWindowSize());
//            for (int i = 0; i < end; i++) {
//                topDocs.scoreDocs[i].score *= context.factor;
//            }

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
                ScoreDoc[] sortedByDocId = new ScoreDoc[topDocs.scoreDocs.length];
                System.arraycopy(topDocs.scoreDocs, 0, sortedByDocId, 0, topDocs.scoreDocs.length);
                Arrays.sort(sortedByDocId, Comparator.comparingInt(a -> a.doc)); // Safe because doc ids >= 0
                Iterator<LeafReaderContext> leaves = searcher.getIndexReader().leaves().iterator();
                LeafReaderContext leaf = null;

                int endDoc = 0;
                for (int i = 0; i < end; i++) {
                    if (topDocs.scoreDocs[i].doc >= endDoc) {
                        do {
                            leaf = leaves.next();
                            endDoc = leaf.docBase + leaf.reader().maxDoc();
                        } while (topDocs.scoreDocs[i].doc >= endDoc);

                        AtomicFieldData fd = context.keyField.load(leaf);
                        if (fd instanceof SortedSetDVBytesAtomicFieldData) {
                            final SortedSetDocValues data = ((SortedSetDVBytesAtomicFieldData) fd).getOrdinalsValues();
                            if (data != null) {
                                if (data.advanceExact(topDocs.scoreDocs[i].doc - leaf.docBase)) {
                                    // document does have data for the field
                                    final String term = data.lookupOrd(data.nextOrd()).utf8ToString();
                                    topDocs.scoreDocs[i].score *= getScoreFactor(term, context.keyPrefix);
                                }
                            }
                        } else if (fd instanceof AtomicNumericFieldData) {
                            final SortedNumericDocValues data = ((AtomicNumericFieldData) fd).getLongValues();
                            if (data != null) {
                                if (!data.advanceExact(topDocs.scoreDocs[i].doc - leaf.docBase)) {
                                    throw new IllegalArgumentException("document [" + topDocs.scoreDocs[i].doc
                                            + "] does not have the field [" + context.keyField.getFieldName() + "]");
                                }
                                if (data.docValueCount() > 1) {
                                    throw new IllegalArgumentException("document [" + topDocs.scoreDocs[i].doc
                                            + "] has more than one value for [" + context.keyField.getFieldName() + "]");
                                }

                                topDocs.scoreDocs[i].score *= getScoreFactor(String.valueOf(data.nextValue()),
                                        context.keyPrefix);
                            }
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
            final String factor;
            if (keyPrefix == null) {
                factor = jedis.get(key);
            } else {
                factor = jedis.get(keyPrefix + key);
            }
            try {
                return Float.parseFloat(factor);
            } catch (NumberFormatException ignored_e) {
                return 1.0f;
            }
        }

        @Override
        public Explanation explain(int topLevelDocId, IndexSearcher searcher, RescoreContext rescoreContext,
                                   Explanation sourceExplanation) throws IOException {
            final RedisRescoreContext context = (RedisRescoreContext) rescoreContext;
            return Explanation.match(1 /* TODO */, context.keyField.getFieldName(), singletonList(sourceExplanation));
        }
    }
}
