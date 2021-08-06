# Elasticsearch Redis Rescore Plugin

Ever wanted to use data from external systems to influence scoring in Elasticsearch? now you can.

The idea is simple: query a low-latency service per document during search and get a numeric value for that specific document, then use that number as a multiplier for influencing the document score, up or down.

This plugin uses Redis to rescore top ranking results in Elasticsearch. It's your job to make sure the query gets the basic filtering and scoring right. Then, you can use the plugin to [rescore](https://www.elastic.co/guide/en/elasticsearch/reference/7.5/search-request-body.html#request-body-search-rescore) the top N results.

Rescoring with this plugin assumes Redis contains keys that can be correlated with the data in the documents, such that for every doc D there exists a value in a predefined field that also exists in Redis as a key whose value is numeric.

Documents that do not contain this field, or no value in this field, or that value does not exist in Redis as a key - are left untouched.

See an example below.

## Installation

Follow the standard plugin installation instructions, with a zip version of the compiled plugin: [https://www.elastic.co/guide/en/elasticsearch/plugins/current/installation.html](https://www.elastic.co/guide/en/elasticsearch/plugins/current/installation.html)

## Usage

```json
{
  "query": { "match_all":  {} },
  "rescore": {
    "redis":{
      "key_field": "productId.keyword",
      "key_prefix": "mystore-",
      "score_operator": "MULTIPLY"
    } 
  } 
}
```

In this example, we are expecting each hit to contain a field `productId` (of keyword type). The value of that field will be looked up in Redis as a key (for example, Redis key `mystore-abc123` will be looked-up for a document with productId abc123; the `mystore-` key prefix is configurable in query time).
The `score_operator` field is the operator you want to be using when doing your final rescore, you can use `ADD`, `MULTIPLY`, or `SUBTRACT`.

The value which will be found under that Redis key, if exists and of numeric type, will be multiplied by the current document score to produce a new score.

You can use `0` to demote results (e.g. mark as unavailable in stock), `1` to leave unchanged, or any other value to produce positive or negative boosts based on your business logic.

<br/>
<br/>

# Plugin Builder and Installation

## 1- First of all pull the git rep into your machine
<br/>

## 2- Install gradle in your machine

<p>In Linux you just have to do: </p>

```bash
sudo apt install gradle
```
<br/>

## 3- Install Java-JDK version 13
In order to compile the plugin, the Java version in your `$Path` must be <strong>version 13</strong> <br/>

If you are using the <strong>Intelij</strong> IDE then you can simply go to: 

> `settings` --> `Build, Execution, Deplyment` --> `Build Tools` --> `Gradle`

And in the `Gradle JVM` field, select the `Dowload JDK` option, there choose the **Version 16** and download.

<br/>

## 4- Install Docker and Redis
Once again, in order for the build to work you need to have the **Docker** installed in you machine, and the **Redis** in it. To do this we can simply run this commands in your terminal:<br/>

```bash
$ sudo apt install docker
$ sudo docker pull redis
```
<br/>

## 5- Build the Plugin