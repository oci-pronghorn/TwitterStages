package com.ociweb.twitter;

import java.util.concurrent.TimeUnit;

import com.ociweb.pronghorn.adapter.twitter.BuildTwitterListsStage;
import com.ociweb.pronghorn.adapter.twitter.GetFollowersStage;
import com.ociweb.pronghorn.adapter.twitter.TwitterEventSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class GatherFollowers {

    
    private static GatherFollowers instance;
    
    
    public static void main(String[] args) {
        
        String consumerKey = getOptArg("consumerKey","-ck",args,null);
        String consumerSecret = getOptArg("consumerSecret","-cs",args,null);
        String token = getOptArg("token","-t",args,null);
        String secret = getOptArg("secret","-s",args,null);
        
        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        
        
        GraphManager gm = new GraphManager();
        
        instance = new GatherFollowers(); 
        
        instance.buildGraph(gm, auth, consumerKey, consumerSecret, token, secret);
        
        //    MonitorConsoleStage.attach(gm);        
        //    GraphManager.enableBatching(gm);
        
        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        
        long start = System.currentTimeMillis();
        //scheduler.playNice=false;
        
        scheduler.startup();  
        
//      try {
//      Thread.sleep(1000);
//  } catch (InterruptedException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//  }
        
        scheduler.awaitTermination(60, TimeUnit.MINUTES);
        
    }

    
    public GraphManager buildGraph(GraphManager gm, Authentication auth, String consumerKey, String consumerSecret, String token, String secret) {
        
        PipeConfig<TwitterEventSchema> config = new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, 400, 2048);
        Pipe<TwitterEventSchema> output = new Pipe<TwitterEventSchema>(config);
        
        GetFollowersStage getFollowersStage = new GetFollowersStage(gm, output, auth, consumerKey, consumerSecret, token, secret, "nathantippy");
        
        PipeCleanerStage<TwitterEventSchema> dump = new PipeCleanerStage<TwitterEventSchema> (gm, output);
        
        
        return gm;
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
