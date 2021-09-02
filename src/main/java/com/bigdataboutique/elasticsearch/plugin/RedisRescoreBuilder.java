package com.bigdataboutique.elasticsearch.plugin;

import com.bigdataboutique.elasticsearch.plugin.exceptions.ScoreFunctionParseException;
import com.bigdataboutique.elasticsearch.plugin.exceptions.ScoreOperatorException;// Exceptions

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import java.util.ArrayList;


import static java.util.Collections.singletonList;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;


public class RedisRescoreBuilder extends RescorerBuilder<RedisRescoreBuilder> {
    public static final String NAME = "redis";

    //------------------------Default Values for the Score and Boost operators-----------------------------
    private static final String SCORE_OPERATOR_DEFAULT = "ADD";
    private static final String BOOST_OPERATOR_DEFAULT = "ADD";
    private static final float BOOST_WEIGHT_DEFAULT = 1f;


    //------------------------------------------------------------------------------------------------------

    protected static final Logger log = LogManager.getLogger(RedisRescoreBuilder.class);

    private final String keyField;
    private final String keyPrefix;
    private final String[] keyPrefixes;
    private final String scoreOperator;
    private final String boostOperator;
    private final Float boostWeight;
    private final Float[] scoreWeights;
    private final String[] scoreFunctions;



    private final String[] possibleOperators = new String[]{"MULTIPLY","ADD","SUBTRACT","SET"};//possible operators

    private static Jedis jedis;

    public static void setJedis(Jedis j) {
        jedis = j;
    }

    public Boolean checkOperator(String operator){ // checks if it's possible to use that operator
        for (String possibleOperator : possibleOperators){
            if (operator.equals(possibleOperator))
                return true;
        }
        return false;
    }
    public static String[] GetStringArray(@Nullable ArrayList<?> arr) { // Transforms a ArrayList in a String[]
        if (arr == null || arr.isEmpty())
            return null;
        String[] str = new String[arr.size()];
        for (int j = 0; j < arr.size(); j++) {
            if(arr.get(j) instanceof String)
                str[j] = (String) arr.get(j);
        }
        return str;
    }

    public static Float[] GetFloatArray(@Nullable ArrayList<?> arr) { // Transforms a ArrayList in a float[]
        if (arr == null || arr.isEmpty())
            return null;
        Float[] res = new Float[arr.size()];
        for (int j = 0; j < arr.size(); j++) {
            if(arr.get(j) instanceof Float)
                res[j] = (Float) arr.get(j);
        }
        return res;
    }




// Constructors--------------------------------------------------------------------------------------------------
    public RedisRescoreBuilder(final String keyField, @Nullable String keyPrefix, @Nullable String scoreOperator,
                               @Nullable String[] keyPrefixes, @Nullable String boostOperator,
                               @Nullable Float boostWeight, @Nullable Float[] scoreWeights,
                               @Nullable String[] scoreFunctions)
            throws ScoreOperatorException {

        this.keyField =  keyField;
        this.keyPrefix = keyPrefix;
        this.scoreOperator = scoreOperator == null ? SCORE_OPERATOR_DEFAULT : scoreOperator;
        this.boostOperator = boostOperator == null ? BOOST_OPERATOR_DEFAULT : boostOperator;
        this.keyPrefixes = keyPrefixes;
        this.boostWeight = boostWeight == null ? BOOST_WEIGHT_DEFAULT : boostWeight;
        this.scoreWeights = scoreWeights;
        this.scoreFunctions = scoreFunctions;


        if (!checkOperator(this.scoreOperator))
            throw new ScoreOperatorException(scoreOperator, "Wrong type operator:");

        else if (!checkOperator(this.boostOperator))
            throw new ScoreOperatorException(boostOperator, "Wrong type operator:");

    }


    public RedisRescoreBuilder(StreamInput in) throws IOException {
        super(in);
        keyField = in.readString();
        keyPrefix = in.readOptionalString();
        scoreOperator = SCORE_OPERATOR_DEFAULT;
        keyPrefixes = null;
        boostOperator = BOOST_OPERATOR_DEFAULT;
        boostWeight = BOOST_WEIGHT_DEFAULT;
        scoreWeights = null;
        scoreFunctions = null;


    }
//--------------------------------------------------------------------------------------------------

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
    private static final ParseField SCORE_OPERATOR = new ParseField("score_operator");
    private static final ParseField KEY_PREFIXES = new ParseField("key_prefixes");
    private static final ParseField BOOST_OPERATOR = new ParseField("boost_operator");
    private static final ParseField BOOST_WEIGHT = new ParseField("boost_weight");
    private static final ParseField SCORE_WEIGHTS = new ParseField("score_weights");
    private static final ParseField SCORE_FUNCTIONS = new ParseField("score_functions");


    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(KEY_FIELD.getPreferredName(), keyField);
        if(scoreOperator != null)
            builder.field(SCORE_OPERATOR.getPreferredName(), scoreOperator);
        if(boostOperator != null)
            builder.field(BOOST_OPERATOR.getPreferredName(), boostOperator);
        if (keyPrefix != null)
            builder.field(KEY_PREFIX.getPreferredName(), keyPrefix);
        if (keyPrefixes != null)
            builder.field(KEY_PREFIXES.getPreferredName(), keyPrefixes);

        builder.field(BOOST_WEIGHT.getPreferredName(), boostWeight);

        builder.field(SCORE_WEIGHTS.getPreferredName(), scoreWeights);

        builder.field(SCORE_FUNCTIONS.getPreferredName(), scoreFunctions);



    }


    private static final ConstructingObjectParser<RedisRescoreBuilder, Void> PARSER =
            new ConstructingObjectParser<RedisRescoreBuilder, Void>(NAME,
            args -> {
                try {
                    return new RedisRescoreBuilder((String) args[0], (String) args[1], (String) args[2],
                            GetStringArray((ArrayList<?>) args[3]) , (String) args[4],
                            (Float) args[5], GetFloatArray((ArrayList<?>) args[6]), GetStringArray((ArrayList<?>) args[7]));

                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            }
            );
    static {
        PARSER.declareString(constructorArg(), KEY_FIELD);
        PARSER.declareString(optionalConstructorArg(), KEY_PREFIX);
        PARSER.declareString(optionalConstructorArg(), SCORE_OPERATOR);
        PARSER.declareStringArray(optionalConstructorArg(), KEY_PREFIXES);
        PARSER.declareString(optionalConstructorArg(), BOOST_OPERATOR);
        PARSER.declareFloat(optionalConstructorArg(), BOOST_WEIGHT);
        PARSER.declareFloatArray(optionalConstructorArg(), SCORE_WEIGHTS);
        PARSER.declareStringArray(optionalConstructorArg(),SCORE_FUNCTIONS);
    }
    public static RedisRescoreBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public RescoreContext innerBuildContext(int windowSize, QueryShardContext context) throws IOException {
        IndexFieldData<?> keyField =
                this.keyField == null ? null : context.getForField(context.fieldMapper(this.keyField));
        return new RedisRescoreContext(windowSize, keyPrefix, keyField, scoreOperator,
                keyPrefixes, boostOperator, boostWeight, scoreWeights, scoreFunctions);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        RedisRescoreBuilder other = (RedisRescoreBuilder) obj;
        return keyField.equals(other.keyField)
                && Objects.equals(keyPrefix, other.keyPrefix)
                && Objects.equals(keyPrefixes, other.keyPrefixes);
    }

    @Override
    public int hashCode() {
        if(keyPrefixes != null)
            return Objects.hash(super.hashCode(), keyField, Arrays.hashCode(keyPrefixes));
        else
            return Objects.hash(super.hashCode(), keyField, keyPrefix);
    }

    String keyField() {
        return keyField;
    }

    @Nullable
    String keyPrefix() {
        return keyPrefix;
    }

    @Nullable
    String[] keyPrefixes(){return keyPrefixes;}

    @Nullable
    String scoreOperator() {
        return scoreOperator;
    }

    @Nullable
    String boostOperator() {
        return boostOperator;
    }

    @Nullable
    Float boostWeight() {
        return boostWeight;
    }

    @Nullable
    Float[] scoreWeights(){
        return scoreWeights;
    }

    @Nullable
    String[] scoreFunctions(){
        return scoreFunctions;
    }


    private static class RedisRescoreContext extends RescoreContext {
        private final String keyPrefix;
        private final String[] keyPrefixes;
        private final String scoreOperator;
        private final String boostOperator;
        private final Float boostWeight;
        private final Float[] scoreWeights;
        private final String[] scoreFunctions;
        @Nullable
        private final IndexFieldData<?> keyField;

        RedisRescoreContext(int windowSize, String keyPrefix, @Nullable IndexFieldData<?> keyField, String scoreOperator,
                            String[] keyPrefixes, String boostOperator, Float boostWeight, Float[] scoreWeights, String[] scoreFunctions) {
            super(windowSize, RedisRescorer.INSTANCE);
            this.keyPrefix = keyPrefix;
            this.keyField = keyField;
            this.scoreOperator = scoreOperator;
            this.keyPrefixes = keyPrefixes;
            this.boostOperator = boostOperator;
            this.boostWeight = boostWeight;
            this.scoreWeights = scoreWeights;
            this.scoreFunctions = scoreFunctions;

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
            assert rescoreContext != null;
            if (topDocs == null || topDocs.scoreDocs.length == 0) {
                return topDocs;
            }

            final RedisRescoreContext context = (RedisRescoreContext) rescoreContext;



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
                SortedSetDocValues docValues = null;
                SortedNumericDocValues numericDocValues = null;
                int endDoc = 0;
                for (int i = 0; i < end; i++) {
                    float redisScore = 0;
                    if (topDocs.scoreDocs[i].doc >= endDoc) {
                        do {
                            leaf = leaves.next();
                            endDoc = leaf.docBase + leaf.reader().maxDoc();
                        } while (topDocs.scoreDocs[i].doc >= endDoc);

                        final AtomicFieldData fd = context.keyField.load(leaf);
                        if (fd instanceof SortedSetDVBytesAtomicFieldData) {
                            docValues = ((SortedSetDVBytesAtomicFieldData) fd).getOrdinalsValues();
                        } else if (fd instanceof AtomicNumericFieldData) {
                            numericDocValues = ((AtomicNumericFieldData) fd).getLongValues();
                        }
                    }

                    if (docValues != null) {
                        if (docValues.advanceExact(topDocs.scoreDocs[i].doc - leaf.docBase)) {
                            // document does have data for the field
                            final String term = docValues.lookupOrd(docValues.nextOrd()).utf8ToString();

                            if (context.keyPrefixes != null){ //keyPrefixes
                                for (int j = 0; j < context.keyPrefixes.length  ; j++){
                                    String prefix = context.keyPrefixes[j];
                                    float scoreWeight = getScoreWeight(context.scoreWeights, j);

                                    if (redisScore == 0)
                                        redisScore = getRescore(term, prefix, scoreWeight, j, context.scoreFunctions);

                                    else {
                                        switch (context.scoreOperator) {
                                            case "ADD":
                                                redisScore += getRescore(term, prefix, scoreWeight,
                                                        j, context.scoreFunctions);
                                                break;
                                            case "MULTIPLY":
                                                redisScore *= getRescore(term, prefix, scoreWeight,
                                                        j, context.scoreFunctions);
                                                break;
                                            case "SUBTRACT":
                                                redisScore -= getRescore(term, prefix, scoreWeight,
                                                        j, context.scoreFunctions);
                                                break;
                                            case "SET":
                                                redisScore = getRescore(term, prefix, scoreWeight,
                                                        j, context.scoreFunctions);
                                                break;
                                        }
                                    }
                                }
                            }
                            else{ // keyPrefix
                                redisScore = getScoreFactor(term, context.keyPrefix,
                                        getScoreWeight(context.scoreWeights,0));
                            }


                        }

                    }
                    else if (numericDocValues != null) {
                        if (!numericDocValues.advanceExact(topDocs.scoreDocs[i].doc - leaf.docBase)) {
                            throw new IllegalArgumentException("document [" + topDocs.scoreDocs[i].doc
                                    + "] does not have the field [" + context.keyField.getFieldName() + "]");
                        }
                        if (numericDocValues.docValueCount() > 1) {
                            throw new IllegalArgumentException("document [" + topDocs.scoreDocs[i].doc
                                    + "] has more than one value for [" + context.keyField.getFieldName() + "]");
                        }
                        if (context.keyPrefixes != null){ //KeyPrefixes
                            for (int j = 0; j < context.keyPrefixes.length  ; j++){
                                String prefix = context.keyPrefixes[j];
                                float scoreWeight = getScoreWeight(context.scoreWeights, j);

                                if (redisScore == 0)
                                    redisScore = getRescore(String.valueOf(numericDocValues.nextValue()),
                                            prefix, scoreWeight, j, context.scoreFunctions);
                                else {
                                    switch (context.scoreOperator) {
                                        case "ADD":
                                            redisScore += getRescore(String.valueOf(numericDocValues.nextValue()),
                                                    prefix, scoreWeight, j, context.scoreFunctions);
                                            break;
                                        case "MULTIPLY":
                                            redisScore *= getRescore(String.valueOf(numericDocValues.nextValue()),
                                                    prefix, scoreWeight, j, context.scoreFunctions);
                                            break;
                                        case "SUBTRACT":
                                            redisScore -= getRescore(String.valueOf(numericDocValues.nextValue()),
                                                    prefix, scoreWeight, j, context.scoreFunctions);
                                            break;
                                        case "SET":
                                            redisScore = getRescore(String.valueOf(numericDocValues.nextValue()),
                                                    prefix, scoreWeight, j, context.scoreFunctions);
                                            break;
                                    }
                                }
                            }
                        }

                        else{ //keyPrefix
                            redisScore = getScoreFactor(String.valueOf(numericDocValues.nextValue()),
                                    context.keyPrefix, getScoreWeight(context.scoreWeights, 0));
                        }
                    }

                    topDocs.scoreDocs[i].score *= context.boostWeight; //apply the boostWeight

                    switch (context.boostOperator) {
                        case "ADD":
                            topDocs.scoreDocs[i].score += redisScore;
                            break;
                        case "MULTIPLY":
                            topDocs.scoreDocs[i].score *= redisScore;
                            break;
                        case "SUBTRACT":
                            topDocs.scoreDocs[i].score -= redisScore;
                            break;
                        case "SET":
                            topDocs.scoreDocs[i].score = redisScore;
                            break;

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

        private static float getRescore(final String key, @Nullable final String keyPrefix, @Nullable float scoreWeight,
                                        @Nullable int keyPrefixesIndex, @Nullable String[] scoreFunctions)
                throws ScoreFunctionParseException {

            if (scoreFunctions == null || scoreFunctions.length == 0)
                return getScoreFactor(key, keyPrefix, scoreWeight);
            //add exception here
            float scoreFactor = getScoreFactor(key, keyPrefix,1);
            Float res = null;

            if( keyPrefixesIndex >= scoreFunctions.length || Objects.equals(scoreFunctions[keyPrefixesIndex], "null")){
                return scoreFactor * scoreWeight;
            }

            try {
                String[] parsed = ScoreFunctionParser.getScoreFunctionParser().parse(scoreFunctions[keyPrefixesIndex],
                        String.valueOf(scoreFactor));


                //--------------------------------------------Functions---------------------------------------------------
                switch (parsed[0]) {
                    case "pow":
                        res = ScoreFunctionsObj.get().pow(Float.parseFloat(parsed[1]), Float.parseFloat(parsed[2]));
                        break;
                }
                //--------------------------------------------------------------------------------------------------------
            } catch(Exception e){
                throw new ScoreFunctionParseException(scoreFunctions[keyPrefixesIndex]);
            }

            return res == null ? scoreFactor * scoreWeight : res * scoreWeight ;
        }


        //ScoreWeight Default Value must be 1
        private static float getScoreFactor(final String key, @Nullable final String keyPrefix,float scoreWeight) {
            assert key != null;

            return AccessController.doPrivileged((PrivilegedAction<Float>) () -> {
                final String fullKey = fullKey(key, keyPrefix);
                final String factor = jedis.get(fullKey);
                if (factor == null) {
                    log.debug("Redis rescore factor null for key " + keyPrefix + key);
                    return 1.0f;
                }

                try { // Here
                    return Float.parseFloat(factor) * scoreWeight;

                } catch (NumberFormatException ignored_e) {
                    log.warn("Redis rescore factor NumberFormatException for key " + fullKey);
                    return 1.0f;
                }
            });
        }

        private float getScoreWeight(@Nullable Float[] scoreWeights, int index){
            if(scoreWeights == null || scoreWeights.length == 0)
                return 1f;
            float size = (float) scoreWeights.length;
            if(index >= size)
                return 1f;
            return scoreWeights[index];
        }

        private static String fullKey(final String key, @Nullable final String keyPrefix) {
            if (keyPrefix == null) {
                return key;
            } else {
                return keyPrefix + key;
            }
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
                float score = getScoreFactor(term, context.keyPrefix, 1f);
                return Explanation.match(score, fieldName, singletonList(sourceExplanation));
            } else {
                return Explanation.noMatch(fieldName, sourceExplanation);
            }
        }
    }
}
