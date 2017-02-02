package com.ociweb.twitter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.TimeUnit;

import com.ociweb.pronghorn.adapter.twitter.AboveFieldValueFilterStage;
import com.ociweb.pronghorn.adapter.twitter.BuildTwitterListsStage;
import com.ociweb.pronghorn.adapter.twitter.FlagFilterStage;
import com.ociweb.pronghorn.adapter.twitter.FollowStage;
import com.ociweb.pronghorn.adapter.twitter.HosebirdFeedStage;
import com.ociweb.pronghorn.adapter.twitter.LanguageFilterStage;
import com.ociweb.pronghorn.adapter.twitter.PassRepeatsFilterStage;
import com.ociweb.pronghorn.adapter.twitter.PassUniquesFilterTwoStepStage;
import com.ociweb.pronghorn.adapter.twitter.TextContentRouterStage;
import com.ociweb.pronghorn.adapter.twitter.TwitterEventSchema;
import com.ociweb.pronghorn.adapter.twitter.UnfollowStage;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.route.SplitterStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;
import com.ociweb.pronghorn.util.BloomFilter;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;

public class WatchFeedForBadFollowers {
    
    //TODO: Collect sorted list of twitter users by most followed, take top of list every 87 seconds, this is more effective than current design.
    //TODO: plus follow back all those who follow, retweet and star?
    
    
    private static WatchFeedForBadFollowers instance;
    
    private final static int SMALL_PIPE = 8;//32;
    private final static int LARGE_PIPE = 16;//64;
    private final static int HOLDING_PIPE = 512;//64;
    
    private final static int BLOB_SIZE = 256;
    
    static {
        System.setProperty("org.apache.commons.logging.Log",
                           "org.apache.commons.logging.impl.NoOpLog");
    }
    
    public static void main(String[] args) {
        
        String consumerKey = getOptArg("consumerKey","-ck",args,null);
        String consumerSecret = getOptArg("consumerSecret","-cs",args,null);
        String token = getOptArg("token","-t",args,null);
        String secret = getOptArg("secret","-s",args,null);
        
        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        
        
        GraphManager gm = new GraphManager();
        
        instance = new WatchFeedForBadFollowers(); 
        
        instance.buildGraph(gm, auth, consumerKey, consumerSecret, token, secret);
        
        MonitorConsoleStage.attach(gm);  //TODO: run timed to get results.
        
        //    GraphManager.enableBatching(gm);
        
        final ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        
        //scheduler.playNice=false;
        
        scheduler.startup();  
        
        //TODO: formalize this pattern
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
              scheduler.shutdown();
              scheduler.awaitTermination(60, TimeUnit.MINUTES);
            }
        });
        
//        try {
//            Thread.sleep(40_000);
//        } catch (InterruptedException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        scheduler.shutdown();
//        scheduler.awaitTermination(60, TimeUnit.MINUTES);
//        
//        System.exit(0);
        
    }

    private BloomFilter loadFilter(File file) throws FileNotFoundException, IOException, ClassNotFoundException {
        FileInputStream fist = new FileInputStream(file);
        ObjectInputStream oist = new ObjectInputStream(fist);                
        BloomFilter localFilter = (BloomFilter) oist.readObject();
         
        oist.close();
        return localFilter;
    }
    
    public GraphManager buildGraph(GraphManager gm, Authentication auth, String consumerKey, String consumerSecret, String token, String secret) {

        
//        "#startup","#startups","#bigdata","data","#iot","#tech","business","internet","#business","Marketing",
//        "#technology","#entrepreneur","Things","code","Learn","Tool Kit","#cloud","Entrepreneur","source","software",
//        "platform","development","#security","solution","technology","analytics","entrepreneurs","product","Enterprise",
//        "Innovation","Small Business","Startups","#Digital","modern","management","big data","Elon","Digital","IoT","future","#technews",
//        "#leadership","Secure",
        //,#entrepreneur,computing,#tech,
        
//        String terms = 
//                "Social,Strategy,Founder,Partner,Investor,Managing,incubator,disrupting,industry,Consultant,Speaker,Trainer,"+
//                "venture,capital,executive,Real-time,RealTime,Revenue,CoFounder,Bitcoin,Blockchain,innovate,"+
//                "collaborate,transform,crowdfund,tech,iot,iiot,machinelearning,bots,chatbots,bigdata,analytics,manufacture,"
//                + "startup,hacking,entrepreneurship,linux,ubuntu,CPU,PCIe,"
//                + "100%,followback,strangeloop,strangeloop_stl,scala,clojure,lambda,monad,"
//                + "startups,data,iot,iiot,tech,management,internet,future,technology,computing,secure,digital,"
//                + "Learn,cloud,development,security,solution,technology,analytics,entrepreneurs,entrepreneur";
            
        String terms = 
                "followback,follow,guru,programming,cloud,functional,"
                +"founder,partner,investor,venture,capital,innovate,innovative,"
                +"Java,Scala,Clojure,IoT,IIOT,digital,webserver,mqtt,RasberryPi,"
                + "IntelMaker,edison,intel,arduino,Raspberry_Pi,embedded,i2c,analog,"
                + "Maker,Galileo,Zulu,AzulSystems,Java8,Java9,seeedstudio,seeed,grovepi,dexter,"
                + "dexterindustries,grovepi+,dexterind,instructables,make,winiot,node-red,"
                + "kickstarter,bigdata,startup,analytics,BBGWireless,BBG,Robotic,BBGW,projects,"
                + "ociweb.com,oci,objectcomputing,nathantippy,pronghorn,github,cloudbees,"
                + "programmable,hackathon,hack,BeagleBone,hackaday,robotercomputer,pi3,pizero,"
                + "BrickPi";
        
        //TODO: need to add anded word lists, words together are more descriptive of the community?
        //      how to cluster users by word choice then choose the target community? this seems ideal.
        
        //TODO: rewrite list as single stage.

//        String[] excludeOnRepeatFilters = new String[]{
//                                                      "bookWords.dat"};

        
        String[] excludeFilters = new String[]{"badWords.dat",
                                               "spanishWords.dat",
                                               "frenchWords.dat"};
        
//        String[] includeFilters = new String[] {"goodWords.dat"                
//                                               };
        
                

        
        
        Pipe<TwitterEventSchema> myFeedPipe             = new Pipe<TwitterEventSchema>(new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, SMALL_PIPE, BLOB_SIZE));
        
        Pipe<TwitterEventSchema> publicFeedPipe         = new Pipe<TwitterEventSchema>(new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, SMALL_PIPE, BLOB_SIZE));
        Pipe<TwitterEventSchema> publicFeedFilteredPipe = new Pipe<TwitterEventSchema>(new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, SMALL_PIPE, BLOB_SIZE)); //This is test to see if tryCopy requires larger target and is missing check?
        Pipe<TwitterEventSchema> doFollowPipe           = new Pipe<TwitterEventSchema>(new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, SMALL_PIPE, BLOB_SIZE));
        
        Pipe<TwitterEventSchema> repeatedBadPipe = new Pipe<TwitterEventSchema>(new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, HOLDING_PIPE, BLOB_SIZE));
     //   Pipe<TwitterEventSchema> repeatedBadPipe2 = new Pipe<TwitterEventSchema>(new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, SMALL_PIPE, BLOB_SIZE));
        Pipe<TwitterEventSchema> nsfwContent = new Pipe<TwitterEventSchema>(new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, HOLDING_PIPE, BLOB_SIZE));
        
        
        Pipe<TwitterEventSchema> feedA = new Pipe<TwitterEventSchema>(new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, LARGE_PIPE, BLOB_SIZE));
        Pipe<TwitterEventSchema> feedB = new Pipe<TwitterEventSchema>(new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, LARGE_PIPE, BLOB_SIZE));
        Pipe<TwitterEventSchema> feedC = new Pipe<TwitterEventSchema>(new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, LARGE_PIPE, BLOB_SIZE));
     //   Pipe<TwitterEventSchema> feedD = new Pipe<TwitterEventSchema>(new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, LARGE_PIPE, BLOB_SIZE));
        
        TwitterFactory factory = new TwitterFactory();        
        Twitter twitter = factory.getInstance();
        try {
            
            twitter.setOAuthConsumer(consumerKey, consumerSecret);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //TODO: find new followers and follow them back.
        
        //TODO: Must filter on region/location.
        
        //TODO:  1 - build list at same time as populate to ensure its sill valid, test.
        //TODO:  2 - record all new follows to file, replay later and follow
        //TODO:  3 - open links for each of the follow later ids.
        
        //TODO: rebuild parse and check with # and without in all cases.
        
        long oneSec = 1_000_000_000;

        boolean findFollowers = true;
        if (findFollowers) {
            GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 1_000_000, new HosebirdFeedStage(gm, auth, terms, publicFeedPipe,7000));

            //removing this one only causes a later point to  fail
            
            GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 200_000_000, new AboveFieldValueFilterStage<TwitterEventSchema> (gm, publicFeedPipe, publicFeedFilteredPipe, //toFollowPipe,
                                                                                                             TwitterEventSchema.MSG_USERPOST_101_FIELD_FRIENDSCOUNT_56,
                                                                                                             TwitterEventSchema.MSG_USERPOST_101_FIELD_FOLLOWERSCOUNT_55,//must have .75% of friends
                                                                                                             900 )); 
                        
            Pipe<TwitterEventSchema> latinPipe0 = latinOnlyFilter(gm, publicFeedFilteredPipe, TwitterEventSchema.MSG_USERPOST_101_FIELD_SCREENNAME_53);
            Pipe<TwitterEventSchema> latinPipe1 = latinOnlyFilter(gm, latinPipe0, TwitterEventSchema.MSG_USERPOST_101_FIELD_DESCRIPTION_58);
            Pipe<TwitterEventSchema> latinPipe2 = latinOnlyFilter(gm, latinPipe1, TwitterEventSchema.MSG_USERPOST_101_FIELD_TEXT_22);
            
                       
            
            //exclude those with any excluded words
            Pipe<TwitterEventSchema> f1 = exlusionFilters(gm, excludeFilters, new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, SMALL_PIPE, BLOB_SIZE), TwitterEventSchema.MSG_USERPOST_101_FIELD_TEXT_22, latinPipe2);
            Pipe<TwitterEventSchema> toFollowPipe = exlusionFilters(gm, excludeFilters, new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, SMALL_PIPE, BLOB_SIZE), TwitterEventSchema.MSG_USERPOST_101_FIELD_DESCRIPTION_58, f1);   
            
//            Pipe<TwitterEventSchema> hasKeyWordsPipe0 = inclusionFilters(gm, includeFilters, new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, SMALL_PIPE, BLOB_SIZE), TwitterEventSchema.MSG_USERPOST_101_FIELD_DESCRIPTION_58, toFollowPipe, false);            
//            Pipe<TwitterEventSchema> hasKeyWordsPipe = inclusionFilters(gm, includeFilters, new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, SMALL_PIPE, BLOB_SIZE), TwitterEventSchema.MSG_USERPOST_101_FIELD_TEXT_22, hasKeyWordsPipe0, false); 
//            
                        
            //TODO: must add support to follow back followers
            
//            GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 3_000_000_000L,new PassUniquesFilterStage<TwitterEventSchema>(gm, hasKeyWordsPipe,
//                                                                                                                doFollowPipe,
//                                                                                                                TwitterEventSchema.MSG_USERPOST_101_FIELD_SCREENNAME_53,
//                                                                                                                new File("seenCounts.dat")));
            
            Pipe<RawDataSchema> didFollowPipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance,LARGE_PIPE,1024));
            
            GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 10_000_000_000L,new PassUniquesFilterTwoStepStage<TwitterEventSchema>(gm, toFollowPipe /*hasKeyWordsPipe*/, didFollowPipe,
                    doFollowPipe,
                    TwitterEventSchema.MSG_USERPOST_101_FIELD_SCREENNAME_53,
                    new File("seenCounts.dat")));

            
            //PassUniquesFilterTwoStepStage
            
            
            // Regulator can only limit the volume to 1 msg per ms or 1000 messages per second, For Twitter we must slow things down to an even greater extent. 
            // Pipe.setConsumerRegulation(doFollowPipe, 1, Pipe.sizeOf(doFollowPipe, TwitterEventSchema.MSG_USERPOST_101_FIELD_SCREENNAME_53));
            
                   
            //once per hour-3600  20min-120
          //  GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 1_000_000_000L, new BuildTwitterListsStage(gm, doFollowPipe, "doFollow", false, consumerKey, consumerSecret, token, secret, twitter)); //once every 60 seconds, once a minute
            
            //once every 87 seconds to do 1000 every day. MUST not be less than 60. hacked to go a bit faster than 87 96ok but ran into trouble.
            GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 100_000_000_000L, new FollowStage(gm, doFollowPipe, didFollowPipe, consumerKey, consumerSecret, token, secret, twitter));
            //testing 900 per day at 96 seconds
            
            //tested 713 per day, or 122 seconds (just over 2 minutes) works fine
        }
        
        //my feed is about 150Kbps
        boolean flagBad = true;
        if (flagBad) {     
            
            //TODO: add another block to look at the followers and find the words  "help" "join" "annoyed" "frustrated" "looking for"
            
            
            
            GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 3_000_000, new HosebirdFeedStage(gm, auth, myFeedPipe,0));
            GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 3_000_000L, new SplitterStage<>(gm, myFeedPipe, feedA, feedB, feedC));
            
  //          GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 7_000_000L,new PipeCleanerStage(gm, feedA));
  //          GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 7_000_000L,new PipeCleanerStage(gm, feedB));
  //          GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 7_000_000L,new PipeCleanerStage(gm, feedC));
   //         GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 7_000_000L,new PipeCleanerStage(gm, feedD));
            
    //        
    //        //GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 21_000_000L,new ConsoleJSONDumpStage<>(gm, toUnfollowListWatch));
    //        //GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 11_000_000L,new ConsoleSummaryStage(gm, toUnfollowListWatch));
    //        GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 7_000_000L,new PipeCleanerStage(gm, toUnfollowListWatch));
    
     //       GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 3_000_000L,new WordFrequencyStage(gm, feedD));
            
            //include anyone using an excluded word
            Pipe<TwitterEventSchema> fActivePipe = inclusionFilters(gm, excludeFilters, new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, SMALL_PIPE, BLOB_SIZE), TwitterEventSchema.MSG_USERPOST_101_FIELD_TEXT_22, feedA, false);     
            GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 5_000_000_000L, new PassRepeatsFilterStage(gm,fActivePipe,repeatedBadPipe,
                    3,
                    TwitterEventSchema.MSG_USERPOST_101_FIELD_SCREENNAME_53,
                    new File("badCounts.dat")));
                       
                      
            
            Pipe<TwitterEventSchema> badDescription = inclusionFilters(gm, excludeFilters, new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, LARGE_PIPE, BLOB_SIZE), TwitterEventSchema.MSG_USER_100_FIELD_DESCRIPTION_58, feedB, false); 
            //GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 7_000_000L,new PipeCleanerStage(gm, badDescription));
            
                       
            
            GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 5_000_000_000L, new FlagFilterStage(gm, feedC, nsfwContent, TwitterEventSchema.MSG_USERPOST_101_FIELD_FLAGS_31, TwitterEventSchema.FLAG_POSSIBLY_SENSITIVE));
            
            
//            Pipe<TwitterEventSchema> fActivePipe2 = inclusionFilters(gm, excludeOnRepeatFilters, new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, SMALL_PIPE, BLOB_SIZE), TwitterEventSchema.MSG_USERPOST_101_FIELD_TEXT_22, feedD, false);     
//            GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 5_000_000_000L, new PassRepeatsFilterStage(gm,fActivePipe2,repeatedBadPipe2,
//                    4,
//                    TwitterEventSchema.MSG_USERPOST_101_FIELD_SCREENNAME_53,
//                    new File("badCountsBooks.dat")));
            
            
            //GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 21_000_000L,new ConsoleJSONDumpStage<>(gm, nsfwContent));
            
            
            //TODO: add language filter
            //TODO: add merge of N feeds into one.
            
            
//            //once every 3 minutes
//            GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 180_000_000_000L, new BuildTwitterListsStage(gm, "unfollow", false, consumerKey, consumerSecret, token, secret, twitter, 
//                    repeatedBadPipe, badDescription, nsfwContent));
           
            
            
            
            //  GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 21_000_000L, new PipeCleanerStage(gm, toUnfollowListLive));
            
            //240    - test 2x faster 120 ok 
           GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 100L * oneSec, new UnfollowStage(gm, consumerKey, consumerSecret, token, secret, twitter, 
                                repeatedBadPipe, nsfwContent, badDescription));
            
            //TODO: watch what is posted
            //TODO: watch posted text and catigorize
            //      list those that should be unfollowed
            //     need bays word list to filter on positive and negative.
            
            //TODO: review any URLS and go to them to determine another filter criteria.
           
            //TODO: must lookup old list and resume when possible?, must ask list for count when resuming.
            
            //
        }
        return gm;
    }

    private Pipe<TwitterEventSchema> latinOnlyFilter(GraphManager gm, Pipe<TwitterEventSchema> publicFeedFilteredPipe,
            int fieldLOC) {
        Pipe<TwitterEventSchema> intlPipe = new Pipe<TwitterEventSchema>(new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, SMALL_PIPE, BLOB_SIZE));
        Pipe<TwitterEventSchema> latinPipe = new Pipe<TwitterEventSchema>(new PipeConfig<TwitterEventSchema>(TwitterEventSchema.instance, SMALL_PIPE, BLOB_SIZE));
        GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 10_000_000, new LanguageFilterStage(gm, publicFeedFilteredPipe, new Pipe[]{latinPipe, intlPipe}, fieldLOC));
        GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 10_000_000, new PipeCleanerStage<>(gm, intlPipe));
        return latinPipe;
    }

    private Pipe<TwitterEventSchema> exlusionFilters(GraphManager gm, String[] filters, PipeConfig<TwitterEventSchema> config, int fieldLOC, Pipe<TwitterEventSchema> inputPipe) {
        Pipe<TwitterEventSchema> fActivePipe=inputPipe;
        
        int j = filters.length;
        while (--j>=0) {
            try {
                Pipe<TwitterEventSchema> fPipe = new Pipe<TwitterEventSchema>(config);
                Pipe<TwitterEventSchema> dumpPipe = new Pipe<TwitterEventSchema>(config);
                GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 200_000_000L, new TextContentRouterStage(gm, fActivePipe, new Pipe[] {fPipe, dumpPipe},fieldLOC, loadFilter(new File(filters[j]))));
                GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 100_000_000L, new PipeCleanerStage<TwitterEventSchema>(gm, dumpPipe));
                //GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 1_000_000_000L, new ConsoleJSONDumpStage<TwitterEventSchema>(gm, dumpPipe));
                
                fActivePipe=fPipe;
            } catch (Exception e) {
                e.printStackTrace();
            }            
        }
        return fActivePipe;
    }
    
    private Pipe<TwitterEventSchema> inclusionFilters(GraphManager gm, String[] filters, PipeConfig<TwitterEventSchema> config, int fieldLOC, Pipe<TwitterEventSchema> outputPipe, boolean debug) {
        
        int j = filters.length;
        BloomFilter[] bFilters = new BloomFilter[j]; 
        while (--j>=0) {
            try {
                bFilters[j] = loadFilter(new File(filters[j]));
                
                if (debug) {
                    System.out.println("filter "+j+" is "+filters[j]);
                }
                
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
        GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 100_000_000L, new TextContentRouterStage(debug, gm, outputPipe, new Pipe[] {dumpPipe, fPipe},fieldLOC, bFilters ));
        GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 300_000_000L, new PipeCleanerStage<TwitterEventSchema>(gm, dumpPipe));
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
