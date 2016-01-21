package com.ociweb.pronghorn.adapter.twitter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
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
 * 
 * 
 * @author Nathan Tippy
 *
 */
public class HosebirdFeedStage extends PronghornStage{

    private final Authentication auth;
    private final StatusesFilterEndpoint endpoint;
    
    private final PublicObjectFactory factory = new PublicObjectFactory(new ConfigurationBuilder().build());
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
    private BasicClient client;
    private Pipe<TwitterEventSchema> output;

    public HosebirdFeedStage(GraphManager graphManager, Authentication auth, String terms, Pipe<TwitterEventSchema> output) {
        super(graphManager, NONE, output);
                
        this.auth = auth;   //new OAuth1(consumerKey, consumerSecret, token, secret);               
        
        StatusesFilterEndpoint localEndpoint = new StatusesFilterEndpoint();
        localEndpoint.addPostParameter(Constants.TRACK_PARAM, terms); //single comma separated string
        this.endpoint = localEndpoint;
        this.output = output;
    }
    
    @Override
    public void startup() {
        
        // Create a new BasicClient. By default gzip is enabled.
        client = new ClientBuilder()
          .hosts(Constants.STREAM_HOST)
          .endpoint(endpoint)
          .authentication(auth)
          .processor(new StringDelimitedProcessor(queue))
          .build();
        
        client.connect();
        
    }


    @Override
    public void run() {
        
            
        while (!client.isDone() && !queue.isEmpty() && PipeWriter.hasRoomForFragmentOfSize(output, TwitterEventSchema.MSG_EVENT_3)    ) {
            
            try {                
                String message = queue.take();
                
                JSONObject json = new JSONObject(message);
                
                if (JSONObjectType.determine(json) == JSONObjectType.Type.STATUS) {
                    Status status = factory.createStatus(json);
                    User user = status.getUser();
                      
                    
                    boolean ok = PipeWriter.tryWriteFragment(output, TwitterEventSchema.MSG_EVENT_3);
                    assert(ok) : "This was prechecked so should not have failed";
                                        
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
                    PipeWriter.writeInt(output, TwitterEventSchema.MSG_EVENT_3_FIELD_FLAGS_31, flags);
                    
                    PipeWriter.writeLong(output, TwitterEventSchema.MSG_EVENT_3_FIELD_ID_32, status.getId());
                    
                    PipeWriter.writeInt(output,  TwitterEventSchema.MSG_EVENT_3_FIELD_FOLLOWERSCOUNT_33, user.getFollowersCount());
                    PipeWriter.writeInt(output,  TwitterEventSchema.MSG_EVENT_3_FIELD_FOLLOWINGCOUNT_34, user.getFavouritesCount());
                    PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_EVENT_3_FIELD_NAME_35, user.getName());
                    PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_EVENT_3_FIELD_SCREENNAME_36, user.getScreenName());
                    PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_EVENT_3_FIELD_DESCRIPTION_37, user.getDescription());                    
                    PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_EVENT_3_FIELD_LOCATION_38, user.getLocation());                    
                    PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_EVENT_3_FIELD_LANGUAGE_39, user.getLang());
                    PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_EVENT_3_FIELD_TIMEZONE_40, user.getTimeZone());
                    PipeWriter.writeUTF8(output, TwitterEventSchema.MSG_EVENT_3_FIELD_TEXT_41, status.getText());
                    
                    PipeWriter.publishWrites(output);
                
                }
                
            } catch (InterruptedException | JSONException | TwitterException e) {
                
                e.printStackTrace();
               return;
                
                //throw new RuntimeException(e);
            }
        }        
                
    }

}
