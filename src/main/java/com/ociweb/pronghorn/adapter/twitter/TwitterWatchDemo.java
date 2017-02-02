package com.ociweb.pronghorn.adapter.twitter;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterWatchDemo {
    
    
    private final Authentication auth;
    private final String terms;

    private TwitterWatchDemo(String terms, Authentication auth) {
        this.terms = terms;
        this.auth = auth;
    }
    
    public static void main(String[] args) {
        
        String consumerKey = getOptArg("consumerKey","-ck",args,null);
        String consumerSecret = getOptArg("consumerSecret","-cs",args,null);
        String token = getOptArg("token","-t",args,null);
        String secret = getOptArg("secret","-s",args,null);
        String terms = getOptArg("terms","-r",args,"Java,BigData");
        
        TwitterWatchDemo app = new TwitterWatchDemo(terms, new OAuth1(consumerKey, consumerSecret, token, secret));
        
        GraphManager gm = new GraphManager();
        app.buildGraph(gm);
        
        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        scheduler.startup();
        
        //When to shut down?
        //scheduler.shutdown();
        
    }

    private void buildGraph(GraphManager gm) {
        
        PipeConfig<TwitterEventSchema> config = new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, 20, 2048);
        
        Pipe<TwitterEventSchema> retweeterPipe = new Pipe<TwitterEventSchema>(config);
        
        HosebirdFeedStage feed = new HosebirdFeedStage(gm, auth, terms, retweeterPipe,0);
        PronghornStage consume = new ConsoleJSONDumpStage(gm, retweeterPipe);
 
    }
    
    private static String getOptArg(String longName, String shortName, String[] args, String defaultValue) {
        String prev = null;
        for (String token : args) {
            if (longName.equals(prev) || shortName.equals(prev)) {
                if (token == null || token.trim().length() == 0 || token.startsWith("-")) {
                    return defaultValue;
                }
                return token.trim();
            }
            prev = token;
        }
        return defaultValue;
    }
    
    
    

}
