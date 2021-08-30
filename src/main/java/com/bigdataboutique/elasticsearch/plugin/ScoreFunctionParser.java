package com.bigdataboutique.elasticsearch.plugin;
import java.util.ArrayList;
import java.util.List;

public class ScoreFunctionParser {
    private static ScoreFunctionParser _scoreFunctionParser;
    private ScoreFunctionParser(){}

    public static ScoreFunctionParser getScoreFunctionParser(){
        if(_scoreFunctionParser == null)
            _scoreFunctionParser = new ScoreFunctionParser();
        return _scoreFunctionParser;
    }

    public List<String> parse(String toParse){
        List<String> res = new ArrayList<>();
        String function;
        int i = 0;
        StringBuilder builder = new StringBuilder();

        while (toParse.charAt(i) != '(') { // GET THE FUNCTION
            builder.append(toParse.charAt(i));
            i++;
        }
        res.add(builder.toString());
        builder.setLength(0);
        i++;
        while (i < toParse.length()){ // params
            char current = toParse.charAt(i);
            if (current == ',' || current == ')'){
                res.add(builder.toString());
                builder.setLength(0);
            }
            else
                builder.append(toParse.charAt(i));
            i++;
        }
        return res;

    }
}
