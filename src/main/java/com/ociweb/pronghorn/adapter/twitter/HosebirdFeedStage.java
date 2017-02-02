package com.ociweb.pronghorn.adapter.twitter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Constants.FilterLevel;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.twitter.hbc.core.endpoint.UserstreamEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;

import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.JSONObjectType;
import twitter4j.PublicObjectFactory;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

/**
 * NOTE: TODO: this stage is not yet garbage free and will require more work to make this possible.
 * 
 * @author Nathan Tippy
 *
 */
public class HosebirdFeedStage extends PronghornStage{

    private final Authentication auth;
    private final StreamingEndpoint endpoint;
    
    private final PublicObjectFactory factory = new PublicObjectFactory(new ConfigurationBuilder().build());
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
    private BasicClient client;
    private Pipe<TwitterEventSchema> output;
    private final String host;
    private final boolean isMyFollowers;

    public HosebirdFeedStage(GraphManager graphManager, Authentication auth, String terms, Pipe<TwitterEventSchema> output, int delay) {
        this(graphManager, auth, terms, output, Constants.STREAM_HOST, delay);
    }
    
    public HosebirdFeedStage(GraphManager graphManager, Authentication auth, Pipe<TwitterEventSchema> output, int delay) {
        //feed of what everyone this user follows is up to.
        this(graphManager, auth, null, output, Constants.USERSTREAM_HOST, delay);
    }
    
    public HosebirdFeedStage(GraphManager graphManager, Authentication auth, String terms, Pipe<TwitterEventSchema> output, String host, int delay) {
        super(graphManager, NONE, output);
                
        this.auth = auth;   //new OAuth1(consumerKey, consumerSecret, token, secret);               
        
        if (null!=terms) {
            isMyFollowers = false;
            StatusesFilterEndpoint localEndpoint = new StatusesFilterEndpoint();
            localEndpoint.addPostParameter(Constants.TRACK_PARAM, terms); //single comma separated string
            //localEndpoint.filterLevel(FilterLevel.Medium);            
            
            this.endpoint = localEndpoint;
        } else {
            isMyFollowers = true;
            UserstreamEndpoint localEndpoint = new UserstreamEndpoint();
            
            localEndpoint.withFollowings(true);
            localEndpoint.withUser(false);
            localEndpoint.allReplies(true);
          //  localEndpoint.filterLevel(FilterLevel.Medium);
            
            this.endpoint = localEndpoint;
        }
        this.output = output;
        
        this.host = host;
        this.delay = delay;
    }
    private final int delay;
    
    @Override
    public void startup() {
        
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
        }
        
        // Create a new BasicClient. By default gzip is enabled.
        client = new ClientBuilder()
          .connectionTimeout(20_000) //default was 4 seconds
          .socketTimeout(240_000) //default 60 seconds
          .retries(20) //default 5
          .hosts(host)
          .endpoint(endpoint)
          .authentication(auth)
          .processor(new StringDelimitedProcessor(queue))
          .build();
       
        client.connect();
        
    }
    
    @Override
    public void shutdown() {
        client.stop();
    }

    @Override
    public void run() {
        long now = System.currentTimeMillis();
            
        while (!client.isDone() && !queue.isEmpty() ) {
                       
            try {                
                JSONObject json = new JSONObject(queue.take());
                
                if (JSONObjectType.determine(json) == JSONObjectType.Type.STATUS) {
                    Status status = factory.createStatus(json);
                    User user = status.getUser();                   
                    
                    //we drop hosebird inputs if the pipe is backed up here. drop all users who follow < 100 
                    if (
                        (isMyFollowers ||
                         ( 
                          !status.isPossiblySensitive() &&
                          !status.isTruncated( )&&
                          user.getFriendsCount()>200 && //does follow some back
                         (now-user.getCreatedAt().getTime()) > (60000*60*24*365*2)//account is over two years old 
                         )
                        )
                        &&                            
                        PipeWriter.tryWriteFragment(output, TwitterEventSchema.MSG_USERPOST_101)) {
                     
                        int flags =
                                (status.isPossiblySensitive() ? TwitterEventSchema.FLAG_POSSIBLY_SENSITIVE : 0 ) |
                                (status.isFavorited() ? TwitterEventSchema.FLAG_FAVORITED : 0 ) |
                                (status.isRetweet() ? TwitterEventSchema.FLAG_RETWEET : 0 ) |
                                (status.isRetweeted() ? TwitterEventSchema.FLAG_RETWEETED : 0 ) |
                                (status.isRetweetedByMe() ? TwitterEventSchema.FLAG_RETWEETED_BY_ME : 0 ) |
                                (status.isTruncated() ? TwitterEventSchema.FLAG_TRUNCATED : 0 ) |
                                
                                (user.isProtected() ? TwitterEventSchema.FLAG_USER_PROTECTED : 0 ) |
                                (user.isVerified() ? TwitterEventSchema.FLAG_USER_VERIFIED : 0 ) |
                                (user.isFollowRequestSent() ? TwitterEventSchema.FLAG_USER_FOLLOW_REQUEST_SENT : 0 ) |
                                (user.isGeoEnabled() ? TwitterEventSchema.FLAG_USER_GEO_ENABLED : 0 );
                        
                        PipeWriter.writeInt(output, TwitterEventSchema.MSG_USERPOST_101_FIELD_FLAGS_31, flags);
                        
                        
                        PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_USERPOST_101_FIELD_NAME_52, user.getName());
                        PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_USERPOST_101_FIELD_SCREENNAME_53, user.getScreenName());
    
                        PipeWriter.writeInt(output,  TwitterEventSchema.MSG_USER_100_FIELD_FAVOURITESCOUNT_54, user.getFavouritesCount());
                        PipeWriter.writeInt(output,  TwitterEventSchema.MSG_USERPOST_101_FIELD_FOLLOWERSCOUNT_55, user.getFollowersCount());
                        PipeWriter.writeInt(output,  TwitterEventSchema.MSG_USERPOST_101_FIELD_FRIENDSCOUNT_56, user.getFriendsCount());
                        
                        PipeWriter.writeLong(output,  TwitterEventSchema.MSG_USERPOST_101_FIELD_CREATEDAT_57, user.getCreatedAt().getTime());
                                                                
                        PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_USERPOST_101_FIELD_DESCRIPTION_58, user.getDescription());    
                        PipeWriter.writeInt(output, TwitterEventSchema.MSG_USERPOST_101_FIELD_LISTEDCOUNT_59, user.getListedCount());    
                        
                        
                        PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_USERPOST_101_FIELD_LANGUAGE_60, user.getLang());
                        PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_USERPOST_101_FIELD_TIMEZONE_61, user.getTimeZone());
                        PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_USERPOST_101_FIELD_LOCATION_62, user.getLocation());                    
                        
                        
                        PipeWriter.writeLong(output, TwitterEventSchema.MSG_USERPOST_101_FIELD_POSTID_21, status.getId());
                        PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_USERPOST_101_FIELD_TEXT_22, status.getText());
                        
                        PipeWriter.publishWrites(output);
                    }
                
                }
                
            } catch (InterruptedException | JSONException | TwitterException e) {
                
                e.printStackTrace();
               return;
                
                //throw new RuntimeException(e);
            }
        }        
                
    }

}
