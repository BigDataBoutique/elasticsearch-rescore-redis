# Elasticsearch Redis Rescore Plugin

Ever wanted to use data from external systems to influence scoring in Elasticsearch? now you can.

This plugin uses Redis to rescore top ranking results in Elasticsearch. It's your job to make sure the query gets the basic filtering and scoring right. Then, you can use the plugin to [rescore](https://www.elastic.co/guide/en/elasticsearch/reference/7.5/search-request-body.html#request-body-search-rescore) the top N results.

Rescoring with this plugin assumes Redis contains keys that can be correlated with the data in the documents, such that for every doc D there exists a value in a predefined field that also exists in Redis as a key whose value is numeric.

Documents that do not contain this field, or no value in this field, or that value does not exist in Redis as a key - are left untouched.

See an example below.

##Usage

```json
{
  "query": { "match_all":  {} },
  "rescore": {
    "redis":{
      "key_field": "productId.keyword",
      "key_prefix": "mystore-"
    } 
  } 
}
```

In this example, we are expecting each hit to contain a field `productId` (of keyword type). The value of that field will be looked up in Redis as a key (for example, Redis key `mystore-abc123` wil be looked-up for a document with productId abc123; the `mystore-` key prefix is configurable in query time).

The value which will be found under that Redis key, if exists and of numeric type, will be multiplied by the current document score to produce a new score.

You can use `0` to demote results (e.g. mark as unavailable in stock), `1` to leave unchanged, or any other value to produce positive or negative boosts based on your business logic.