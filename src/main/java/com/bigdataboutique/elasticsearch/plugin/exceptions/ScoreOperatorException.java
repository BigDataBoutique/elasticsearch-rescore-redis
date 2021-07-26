package com.bigdataboutique.elasticsearch.plugin.exceptions;

public class ScoreOperatorException extends Exception {

    public ScoreOperatorException(String operator, String Message) {
        super(Message + " " + operator);
    }

}
