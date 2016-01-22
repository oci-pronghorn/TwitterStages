package com.ociweb.pronghorn.adapter.twitter;

import java.util.Map;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import twitter4j.PagableResponseList;
import twitter4j.RateLimitStatus;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;

public class GetFollowersStage extends PronghornStage {

    
    private Twitter twitter;
    private  Map<String, RateLimitStatus> initRateLimitStatus;
    
    
    //input, accounts to get
    //output, followers of these accounts
    
    
 //TODO: stage in the process of being developed.  Do not use yet.
    
 protected GetFollowersStage(GraphManager graphManager, Pipe input, Pipe output) {
        super(graphManager, input, output);
        
        //TODO: set fixed speed to match Twitter
        
    }

 @Override
 public void startup() {
     twitter = TwitterFactory.getSingleton();
     try {
        initRateLimitStatus = twitter.getRateLimitStatus();
    } catch (TwitterException e) {
        throw new RuntimeException(e);
    }
     
 }
 
 private static final String key = "/followers/list";
 
 private String screenName = null;
 private long nextCursor = -1;

 @Override
 public void run() {
     
     if (null==screenName) {
         //pull value off pipe.
         
     }
     
     
     //15 calls at 20 each is 300 followers which can only be pulled once every 15 minutes so..
     
     if (initRateLimitStatus.get(key).getRemaining()>0 || 0==initRateLimitStatus.get(key).getSecondsUntilReset() ) {
         

         try {
              PagableResponseList<User> usersResponse = twitter.getFollowersList(screenName, nextCursor);
              nextCursor = usersResponse.getNextCursor();
              
              //RateLimitStatus rateLimitStatus = usersResponse.getRateLimitStatus();
              
              for(int i = 0; i<usersResponse.size(); i++) {
                  
                  User u = usersResponse.get(i);
                  
                  u.getName();
                  u.getScreenName();
                  u.getFavouritesCount();
                  u.getFollowersCount();
                  u.getFriendsCount();
                  u.getCreatedAt();
                  u.getDescription();
                  u.getId();
                  u.getLang();
                  u.getTimeZone();
                  u.getLocation();

                  u.isFollowRequestSent();
                  u.isGeoEnabled();
                  u.isProtected();
                  u.isVerified();
                  
                  //also write caller screenName and next cursor for restart from this position.
                  
              }
              
              if (nextCursor<0) {
                  screenName = null;
              }
              
              
         } catch (TwitterException e) {
            e.printStackTrace();
         }

        
        
         
     }

     
     
 }
 
 


}
