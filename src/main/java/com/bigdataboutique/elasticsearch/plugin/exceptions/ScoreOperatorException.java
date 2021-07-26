package com.bigdataboutique.elasticsearch.plugin.exceptions;
import java.io.IOException;


public class ScoreOperatorException extends IOException {

    public ScoreOperatorException(String operator, String Message) {
        super(Message + " " + operator);
    }

}
