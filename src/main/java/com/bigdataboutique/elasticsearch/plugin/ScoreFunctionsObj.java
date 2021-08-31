package com.bigdataboutique.elasticsearch.plugin;
//import java.lang.Math;

public class ScoreFunctionsObj {
    private static ScoreFunctionsObj _scoreFunctions;
    private ScoreFunctionsObj(){}

    public static ScoreFunctionsObj get(){
        if (_scoreFunctions == null )
            _scoreFunctions = new ScoreFunctionsObj();
        return _scoreFunctions;
    }

    //----------------------Functions-------------------------------------
    public float pow(float base, float exponent){
        return (float) Math.pow(base,exponent);
    }
    //-------------------------------------------------------------
}
