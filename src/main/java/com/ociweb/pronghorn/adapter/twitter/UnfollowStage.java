package com.ociweb.pronghorn.adapter.twitter;

import java.util.Date;
import java.util.Random;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.BloomFilter;

import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.auth.AccessToken;

public class UnfollowStage extends PronghornStage {
    
    private Pipe<TwitterEventSchema>[] inputs;
    private Twitter twitter;
    private String token;
    private String secret;
        
    //do not add to list, adding twice is not so bad so we do not need save this one.
    private BloomFilter filter;
    
    public UnfollowStage(GraphManager graphManager, String consumerKey, String consumerSecret, String token, String secret, Twitter twitter, Pipe<TwitterEventSchema> ... inputs) {
        super(graphManager, inputs, NONE);
        this.inputs = inputs;
        
        this.token = token;
        this.secret = secret;
        this.twitter = twitter;
    }
    
    @Override
    public void startup() {
        
        AccessToken accessToken = new AccessToken(token, secret);
        twitter.setOAuthAccessToken(accessToken);
        
        filter = new BloomFilter(20_000, .00001);
    }
    
    private Random r = new Random();
    
    @Override
    public void run() {
        
        int i = inputs.length;
        while (--i >= 0) {
                
            Pipe<TwitterEventSchema> in = inputs[i];
            
            //only unfollow 1
            if (PipeReader.tryReadFragment(in)) {
                if (TwitterEventSchema.MSG_USERPOST_101 == PipeReader.getMsgIdx(in)) {
                    StringBuilder target = new StringBuilder();
                                    
                    //display to user.
                    String value = PipeReader.readUTF8(in, TwitterEventSchema.MSG_USERPOST_101_FIELD_SCREENNAME_53, target).toString();
    
                    if (!filter.mayContain(value)) {                
                        
                       // System.out.println(new Date()+" UNFOLLOW  http://twitter.com/"+value);                
                        System.out.println(new Date()+"Idx:"+i+" UNFOLLOW  https://twitter.com/intent/follow?screen_name="+value);
                        
                        try {
                            //user interaction delay.
                           // Thread.sleep(1000+r.nextInt(3000));
                            twitter.destroyFriendship(value);
                            filter.addValue(value);                            
                            
                        } catch (Throwable e) {
                            System.err.println("dropped: unable to unfollow "+value+" due to "+e.getMessage());
                        } 
                    }   
                    
                }
                PipeReader.releaseReadLock(in);
                return;//only do one
            }
        }

    }

}
//This users content https://twitter.com/KFakieh
// broke the pipe and twitter also has an issue with it https://twitter.com/intent/follow?screen_name=KFakieh

//[UnfollowStage] ERROR com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler - Stacktrace
//java.lang.UnsupportedOperationException: Bad msgId:425 encountered at last absolute position:13311 recent primary ring context:[36, 26, 62, 0, 169031664, -1583972351, 62, 147, 209, 425]
//    at com.ociweb.pronghorn.pipe.StackStateWalker.prepReadMessage2EOF(StackStateWalker.java:387)
//    at com.ociweb.pronghorn.pipe.StackStateWalker.prepReadMessage2(StackStateWalker.java:345)
//    at com.ociweb.pronghorn.pipe.StackStateWalker.prepReadMessage2(StackStateWalker.java:326)
//    at com.ociweb.pronghorn.pipe.StackStateWalker.prepReadMessage(StackStateWalker.java:298)
//    at com.ociweb.pronghorn.pipe.StackStateWalker.prepReadMessage(StackStateWalker.java:139)
//    at com.ociweb.pronghorn.pipe.PipeReader.tryReadFragment(PipeReader.java:559)
//    at com.ociweb.pronghorn.adapter.twitter.UnfollowStage.run(UnfollowStage.java:57)
//    at com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler.runPeriodicLoop(ThreadPerStageScheduler.java:450)
//    at com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler.access$6(ThreadPerStageScheduler.java:422)
//    at com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler$3.run(ThreadPerStageScheduler.java:351)
//    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
//    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
//    at java.lang.Thread.run(Thread.java:745)
//[UnfollowStage] ERROR com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler - Unexpected error in stage UnfollowStage[17]
