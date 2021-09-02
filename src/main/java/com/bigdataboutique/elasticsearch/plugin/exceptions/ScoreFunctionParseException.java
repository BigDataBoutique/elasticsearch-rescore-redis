package com.bigdataboutique.elasticsearch.plugin.exceptions;
import java.io.IOException;

public class ScoreFunctionParseException extends IOException{
    public ScoreFunctionParseException(String func){
        super("Bad function body: "+ func);
    }
}
