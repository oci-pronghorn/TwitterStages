package com.ociweb.twitter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import com.ociweb.pronghorn.adapter.twitter.AboveFieldValueFilterStage;
import com.ociweb.pronghorn.adapter.twitter.BuildTwitterListsStage;
import com.ociweb.pronghorn.adapter.twitter.FollowStage;
import com.ociweb.pronghorn.adapter.twitter.GetFollowersStage;
import com.ociweb.pronghorn.adapter.twitter.HosebirdFeedStage;
import com.ociweb.pronghorn.adapter.twitter.PassRepeatsFilterStage;
import com.ociweb.pronghorn.adapter.twitter.PassUniquesFilterStage;
import com.ociweb.pronghorn.adapter.twitter.TextContentRouterStage;
import com.ociweb.pronghorn.adapter.twitter.TwitterEventSchema;
import com.ociweb.pronghorn.adapter.twitter.UnfollowStage;
import com.ociweb.pronghorn.adapter.twitter.WordFrequencyStage;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.route.SplitterStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.ociweb.pronghorn.stage.test.ConsoleSummaryStage;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;
import com.ociweb.pronghorn.util.BloomFilter;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;

public class FeedCleaner {
    
    private static FeedCleaner instance;
    
    /**
     * startup application arguments
     * -ck <consumerKey>
     * -cs <consumerSecret>
     * -t <token>
     * -s <secret>
     * 
     * @param args
     */
    public static void main(String[] args) {
        
        String consumerKey = getOptArg("consumerKey","-ck",args,null);
        String consumerSecret = getOptArg("consumerSecret","-cs",args,null);
        String token = getOptArg("token","-t",args,null);
        String secret = getOptArg("secret","-s",args,null);
        
        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
                
        GraphManager gm = new GraphManager();
        
        instance = new FeedCleaner(); 
        
        instance.buildGraph(gm, auth, consumerKey, consumerSecret, token, secret);

        final ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        scheduler.startup();  
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
              scheduler.shutdown();
              scheduler.awaitTermination(60, TimeUnit.MINUTES);
            }
        }); 
    }

    private BloomFilter loadFilter(File file) throws FileNotFoundException, IOException, ClassNotFoundException {
        FileInputStream fist = new FileInputStream(file);
        ObjectInputStream oist = new ObjectInputStream(fist);                
        BloomFilter localFilter = (BloomFilter) oist.readObject();         
        oist.close();
        return localFilter;
    }
    
    public GraphManager buildGraph(GraphManager gm, Authentication auth, String consumerKey, String consumerSecret, String token, String secret) {
        Twitter twitter = TwitterFactory.getSingleton();
        try {
            twitter.setOAuthConsumer(consumerKey, consumerSecret);
        } catch (Exception e) {
            e.printStackTrace();
        }

        String[] excludeFilters = new String[]{"badWords.dat",
                                               "spanishWords.dat",
                                               "frenchWords.dat"};

        PipeConfig<TwitterEventSchema> config           = new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, 128, 2048);
               
        Pipe<TwitterEventSchema> myFeedPipe             = new Pipe<TwitterEventSchema>(new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, 16, 2048));
        
        Pipe<TwitterEventSchema> toUnfollowList = new Pipe<TwitterEventSchema>(new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, 640, 2048));
        
        GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 3_000_000, new HosebirdFeedStage(gm, auth, myFeedPipe,0));
        Pipe<TwitterEventSchema> fActivePipe = inclusionFilters(gm, excludeFilters, config, TwitterEventSchema.MSG_USERPOST_101_FIELD_TEXT_22, myFeedPipe);        
        
        GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 5_000_000_000L, new PassRepeatsFilterStage(gm,fActivePipe,toUnfollowList,
                                                                     3,
                                                                     TwitterEventSchema.MSG_USERPOST_101_FIELD_SCREENNAME_53,
                                                                     new File("badCounts.dat")));

        GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 1200_000_000_000L, new BuildTwitterListsStage(gm, "unfollow", false, consumerKey, consumerSecret, token, secret, twitter, toUnfollowList));
        
        return gm;
    }

    private Pipe<TwitterEventSchema> inclusionFilters(GraphManager gm, String[] filters, PipeConfig<TwitterEventSchema> config, int fieldLOC, Pipe<TwitterEventSchema> outputPipe) {
        
        int j = filters.length;
        BloomFilter[] bFilters = new BloomFilter[j]; 
        while (--j>=0) {
            try {
                bFilters[j] = loadFilter(new File(filters[j]));
            } catch (FileNotFoundException e) {
               throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
        Pipe<TwitterEventSchema> fPipe = new Pipe<TwitterEventSchema>(config);
        Pipe<TwitterEventSchema> dumpPipe = new Pipe<TwitterEventSchema>(config);
        GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 2_000_000_000L, new TextContentRouterStage(gm, outputPipe, new Pipe[] {dumpPipe, fPipe},fieldLOC, bFilters ));
        GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 1_000_000_000L, new PipeCleanerStage<TwitterEventSchema>(gm, dumpPipe));
        return fPipe;
        
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
