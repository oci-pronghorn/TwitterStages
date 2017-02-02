package com.ociweb.pronghorn.adapter.twitter;

import java.util.Map;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.twitter.hbc.httpclient.auth.Authentication;

import twitter4j.PagableResponseList;
import twitter4j.RateLimitStatus;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.auth.AccessToken;

public class GetFollowersStage extends PronghornStage {

    
    private Twitter twitter;
    private Map<String, RateLimitStatus> initRateLimitStatus;
    private Pipe<TwitterEventSchema> output;
    
    private int                       userPos;
    private PagableResponseList<User> usersResponse;
    
    //input, accounts to get
    //output, followers of these accounts
    
    
    private static final String key = "/followers/list";
    
    private String screenName = null;
    private long nextCursor = -1;
    private Authentication auth;
    private String consumerKey;
    private String consumerSecret;
    private String token;
    private String secret;
    
 public GetFollowersStage(GraphManager graphManager, Pipe<TwitterEventSchema> output, Authentication auth, 
                          String consumerKey, String consumerSecret, String token, String secret, String screenName) {
        super(graphManager, NONE, output);
        this.output = output;
        this.auth = auth;
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.token = token;
        this.secret = secret;
        this.screenName = screenName;
    }

 @Override
 public void startup() {
     usersResponse = null;
     twitter = TwitterFactory.getSingleton();
     
     twitter.setOAuthConsumer(consumerKey, consumerSecret);
     AccessToken accessToken = new AccessToken(token, secret);
     twitter.setOAuthAccessToken(accessToken);

     
 }


 @Override
 public void run() {
     
     try {
         initRateLimitStatus = twitter.getRateLimitStatus(); //can not ask for this often either so we slow loop down.
     } catch (TwitterException e) {
         throw new RuntimeException(e);
     }     
     
//     //TODO: new code
//     if (null==screenName) {
//         //pull value off pipe.         
//     }     
     
     //15 calls at 20 each is 300 followers which can only be pulled once every 15 minutes so..
     
     if (initRateLimitStatus.get(key).getRemaining()>0 && 0==initRateLimitStatus.get(key).getSecondsUntilReset() ) {
         
         try {
             
              if (null==usersResponse) {    
                  
                  System.out.println("request more data ... ");
                  
                  usersResponse = twitter.getFollowersList(screenName, nextCursor);
                  userPos = 0;
              }
              
              while (userPos < usersResponse.size()) {
                  
                  if (!PipeWriter.tryWriteFragment(output, TwitterEventSchema.MSG_USER_100)) {
                      return;//try again later
                  }
                  
                  User u = usersResponse.get(userPos++);
                  
                  int flags = 0;
                  if (u.isProtected()) {
                      flags |=  TwitterEventSchema.FLAG_USER_PROTECTED;   
                  }
                  if (u.isVerified()) {
                      flags |= TwitterEventSchema.FLAG_USER_VERIFIED;
                  }
                  if (u.isFollowRequestSent()) {
                      flags |= TwitterEventSchema.FLAG_USER_FOLLOW_REQUEST_SENT;
                  }
                  if (u.isGeoEnabled()) {
                      flags |= TwitterEventSchema.FLAG_USER_GEO_ENABLED;
                  }
                  
                  
                   // u.isTranslator();
                   // u.isContributorsEnabled();
                   // u.isProfileBackgroundTiled();
                   // u.isProfileUseBackgroundImage();
                   // u.isShowAllInlineMedia();
                  PipeWriter.writeInt(output, TwitterEventSchema.MSG_USER_100_FIELD_FLAGS_31, flags);                  
                  
                  PipeWriter.writeLong(output, TwitterEventSchema.MSG_USER_100_FIELD_USERID_51, u.getId());                  
                  PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_USER_100_FIELD_NAME_52, u.getName());
                  PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_USER_100_FIELD_SCREENNAME_53, u.getScreenName());         
                  
                  PipeWriter.writeInt(output, TwitterEventSchema.MSG_USER_100_FIELD_FAVOURITESCOUNT_54, u.getFavouritesCount());                  
                  PipeWriter.writeInt(output, TwitterEventSchema.MSG_USER_100_FIELD_FOLLOWERSCOUNT_55, u.getFollowersCount());                  
                  PipeWriter.writeInt(output, TwitterEventSchema.MSG_USER_100_FIELD_FRIENDSCOUNT_56, u.getFriendsCount());
                  
                  PipeWriter.writeLong(output, TwitterEventSchema.MSG_USER_100_FIELD_CREATEDAT_57, u.getCreatedAt().getTime());
                  
                  PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_USER_100_FIELD_DESCRIPTION_58, u.getDescription());
                  PipeWriter.writeInt(output, TwitterEventSchema.MSG_USER_100_FIELD_LISTEDCOUNT_59, u.getListedCount());                  
                  PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_USER_100_FIELD_LANGUAGE_60, u.getLang());
                                    
                  PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_USER_100_FIELD_TIMEZONE_61, u.getTimeZone());
                  PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_USER_100_FIELD_LOCATION_62, u.getLocation());
               
                  PipeWriter.publishWrites(output);
                  
              }
              nextCursor = usersResponse.getNextCursor();
              usersResponse =  null;
              
              if (nextCursor<0) {
                  screenName = null;
              }
              
              
         } catch (TwitterException e) {
            throw new RuntimeException(e);
         }
                 
     } else {
         
         int sec = initRateLimitStatus.get(key).getSecondsUntilReset();
         System.out.println("must wait "+sec+" seconds");
         
         //TODO: what is the rate? set the pipe volume at this speed
         if (sec>0) {
             try {
                Thread.sleep(1000*sec);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
         }
         
     }
     
 }
 
 


}
