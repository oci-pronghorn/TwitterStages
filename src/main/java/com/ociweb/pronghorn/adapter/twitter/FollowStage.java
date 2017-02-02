package com.ociweb.pronghorn.adapter.twitter;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Random;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.RawDataSchema.RawDataConsumer;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.BloomFilter;

import twitter4j.Friendship;
import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.auth.AccessToken;

public class FollowStage extends PronghornStage {
    
    private Pipe<TwitterEventSchema> input;
    private  Pipe<RawDataSchema> didFollowPipe;
    private Twitter twitter;
    private String token;
    private String secret;
        
    //do not add to list, adding twice is not so bad so we do not need save this one.
    private BloomFilter filter;
    
    public FollowStage(GraphManager graphManager, Pipe<TwitterEventSchema> input, Pipe<RawDataSchema> didFollowPipe,  String consumerKey, String consumerSecret, String token, String secret, Twitter twitter) {
        super(graphManager, input, NONE);
        this.input = input;
        this.didFollowPipe = didFollowPipe;
        
        this.token = token;
        this.secret = secret;
        this.twitter = twitter;
    }
    
//    public FollowStage(GraphManager graphManager, Pipe<TwitterEventSchema> input, String consumerKey, String consumerSecret, String token, String secret) {
//        super(graphManager, input, NONE);
//        this.input = input;
//        
//        this.token = token;
//        this.secret = secret;
//        this.twitter = null;
//    }
    
    public class User {
        public String screenName;
        public int followers;
        
        public String toString() {
            return followers+" "+screenName;
        }
    }
    
    private Comparator<User> comp = new Comparator() {

        @Override
        public int compare(Object o1, Object o2) {
            int a = ((User)o1).followers;
            int b = ((User)o2).followers;
            return Integer.compare(b,  a);
        }
        
    };
    
    @Override
    public void startup() {
        if (null!=twitter) {
            AccessToken accessToken = new AccessToken(token, secret);
            twitter.setOAuthAccessToken(accessToken);
        }
        
        filter = new BloomFilter(20_000, .00001);
    }
    
    private Random r = new Random();
    
    @Override
    public void run() {
        
        List<User> screenNames = new ArrayList<User>();
        
        //TODO: add support to keep previous selections in case needed in the future.
        
        while (screenNames.size()<100 && PipeReader.tryReadFragment(input)) {
            if (TwitterEventSchema.MSG_USERPOST_101 == PipeReader.getMsgIdx(input)) {
                StringBuilder target = new StringBuilder();
                                
                //display to user.
                String value = PipeReader.readUTF8(input, TwitterEventSchema.MSG_USERPOST_101_FIELD_SCREENNAME_53, target).toString();
                
                int followingCount = PipeReader.readInt(input, TwitterEventSchema.MSG_USERPOST_101_FIELD_FRIENDSCOUNT_56);
                
                
                if (!filter.mayContain(value)) { 
                    User u = new User();
                    u.screenName = value;
                    u.followers = followingCount;
                    screenNames.add(u);
                    filter.addValue(value);
                                        
                }   
                
            }
            PipeReader.releaseReadLock(input);
        }

        try {
           //TODO: pick the best over this call and follow one. 
            
           // System.out.println("check: "+screenNames);
            
            
            Collections.sort(screenNames, comp);
            
            System.out.println("List:"+screenNames);
            
            long lastCall = 0;
            
            String[] namesArray = new String[screenNames.size()];
            
            int i = screenNames.size();
            while (--i>=0) {
                namesArray[i] = screenNames.get(i).screenName;
            }
            
            ResponseList<Friendship> friendships = null;
            
            do {
                try {
                    friendships = twitter.friendsFollowers().lookupFriendships( namesArray);
                } catch (Exception ex) {
                    //continue to retry every few seconds
                    //this is just a network problem that will clear up soon.
                    try {
                        Thread.sleep(40_000);
                    } catch (InterruptedException e) {
                    }
                }
            } while (null==friendships);            
            
            
            for(Friendship f:friendships) {                
                if (!f.isFollowing()) {
                    
                    
                  try {
                      long waitTime = (lastCall+87000) - System.currentTimeMillis();
                      if (waitTime>0) {
                          Thread.sleep(waitTime);
                      }                      
                      
                      twitter.createFriendship(f.getScreenName());

                      lastCall = System.currentTimeMillis();
                      
                      System.out.println(new Date()+" FOLLOWED https://twitter.com/intent/follow?screen_name="+f.getScreenName());   
                      
                      //if no room just drop this data, its not that critical.
                      if (PipeWriter.tryWriteFragment(didFollowPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1)) {
                          PipeWriter.writeUTF8(didFollowPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, f.getScreenName());
                          PipeWriter.publishWrites(didFollowPipe);
                      }                      
                      
                      
                      //Just do the first one then exit and wait for the next minute
                      return;
                      
                      
                  } catch (TwitterException e) {
                      System.out.println("firefox https://twitter.com/intent/follow?screen_name="+f.getScreenName());    
                      
                      //throw new RuntimeException(e);
                  } catch (InterruptedException e) {
                     
                  }
                    
                    
                } else {
                    //already following but this got by the filter so record it.
                    
                    //if no room just drop this data, its not that critical.
                    if (PipeWriter.tryWriteFragment(didFollowPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1)) {
                        PipeWriter.writeUTF8(didFollowPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2, f.getScreenName());
                        PipeWriter.publishWrites(didFollowPipe);
                    } 
                    
                }
            }
            
        } catch (Throwable e) {
            e.printStackTrace();
            return;
        }
        
        
        
    }

}
