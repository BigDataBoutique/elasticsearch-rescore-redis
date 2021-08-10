package com.bigdataboutique.elasticsearch.plugin.exceptions;
import java.io.IOException;

public class PrefixesOverlapingException extends IOException {
    public PrefixesOverlapingException(String fieldOne, String fieldTwo){
        super("You can only have one of the following fields: "+ "'"+ fieldOne+"'" +", " + "'"+ fieldTwo+"'");
    }
}
