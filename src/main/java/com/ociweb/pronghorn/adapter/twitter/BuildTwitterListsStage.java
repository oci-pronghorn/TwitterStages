package com.ociweb.pronghorn.adapter.twitter;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;

import javax.management.RuntimeErrorException;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.BloomFilter;

import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.UserList;
import twitter4j.auth.AccessToken;

public class BuildTwitterListsStage extends PronghornStage {
    
    private Pipe<TwitterEventSchema>[] input;
    private int activeInputIdx;
    
    private Twitter twitter;
    private String token;
    private String secret;
    
    private UserList list=null;
    private String listName;
    
    private int      listMemberCount;
    private final static int maxMembersPerList = 300;
    private final static int maxMembersPerCall = 21;// stopped, stopped with 64 //need to leave room before 100 if this is called twice.
    
    
    private String listPrefix;
    private boolean isListPublic;
    private int      totalRunningCount = 0;
    private String  listOwnerScreenName = "NathanTippy";
    
    //do not add to list, adding twice is not so bad so we do not need save this one.
    private BloomFilter filter;
    private String[] temp;
    private int waitCount=-1;
    
    public BuildTwitterListsStage(GraphManager graphManager, String listPrefix, boolean isPublic, String consumerKey, String consumerSecret, String token, String secret, Twitter twitter, Pipe<TwitterEventSchema> ... input) {
        super(graphManager, input, NONE);
        this.input = input;
        
        this.token = token;
        this.secret = secret;
        this.listPrefix = listPrefix;
        this.isListPublic = isPublic;
        this.twitter = twitter;
    }
    
    @Override
    public void startup() {
                
        temp = new String[maxMembersPerList];
        
        AccessToken accessToken = new AccessToken(token, secret);
        twitter.setOAuthAccessToken(accessToken);
        
        filter = new BloomFilter(20_000, .00001);
    }

    private void createListIfNeeded() {
        if (null==list) {
            String thisMoment = DateTimeFormatter.ofPattern("dd'T'HH:mmX")
                                                    .withZone(ZoneOffset.systemDefault())
                                                   .format(Instant.now());
            listName = listPrefix + ' ' + thisMoment;
                       
            try {
                list = twitter.createUserList(listName, isListPublic , listPrefix);
            } catch (TwitterException e) {
                throw new RuntimeException(e);
            }
            listMemberCount = 0;
            
            //do not use right away.
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e2) {
                // TODO Auto-generated catch block
                e2.printStackTrace();
            }
            
            
        }
    }
    
    //NOTE: twitter has a queue of 100 items to be added from this user
    //      they apply these async and if this list is full you can not add more
    //      so upon add we must wait for the count to change before adding more.
    //      we can not add more than 100 in one call but we should add less so user can still do manual changes.
    
    private static Object lock = new Object();
    
    @Override
    public void run() {
        //prevent simulantious call.
        synchronized(lock) {

        
            //if we have any pending updates must wait for them to finish before continue
            if (waitCount>=0) {
             
                System.out.println("abandon last update of "+waitCount);
               waitCount = -1;

            }
            
            int minPerCall = 16;
            
            StringBuilder adding = new StringBuilder();

                
                int pos = 0;
                
                int startIdx = activeInputIdx;
                
                do {
                    while ((pos < maxMembersPerCall) && PipeReader.tryReadFragment(input[activeInputIdx]) ) { 
                        if (TwitterEventSchema.MSG_USERPOST_101 == PipeReader.getMsgIdx(input[activeInputIdx])) {
                            //display to user.
                            String value = PipeReader.readUTF8(input[activeInputIdx], TwitterEventSchema.MSG_USERPOST_101_FIELD_SCREENNAME_53, new StringBuilder()).toString().trim();
                            
                            if (0==value.length() || value.contains(" ") || value.contains("//")) {
                                System.out.println("no or bad screen name? found on active input: "+activeInputIdx);
                                PipeReader.printFragment(input[activeInputIdx], System.out);
//                                System.exit(0);
                            } else {
                                if (!filter.mayContain(value)) {                
                                    filter.addValue(value);
                                    adding.append("echo "+listName+"; firefox https://twitter.com/intent/follow?screen_name="+value);                
                                    temp[pos++] = value;
                                }   
                            }
                        } else {
                            System.out.println("did not expect msg "+PipeReader.getMsgIdx(input[activeInputIdx]));
                        }
                        PipeReader.releaseReadLock(input[activeInputIdx]);
                    }
                
                    if (++activeInputIdx == input.length) {
                        activeInputIdx = 0;
                    }
                
                
                } while (pos < maxMembersPerCall && activeInputIdx!=startIdx);
                
                if (pos>0) {
                    totalRunningCount += pos;
                   // System.out.println(listPrefix+" now adding "+pos+" for running total of "+totalRunningCount);
                    
                    if (listMemberCount+pos >= maxMembersPerList) {
                        list=null;
                    }
                    createListIfNeeded();
                    
                    int backoff = 1; //TODO: re implement back off without sleep.
                    do {
                        try {                    
                            Thread.sleep(backoff);
                            
                            list = twitter.createUserListMembers(list.getId(), Arrays.copyOfRange(temp, 0, pos));
                            if (list.getMemberCount() > listMemberCount) {
                                backoff = 0; 
                               // System.out.println(new Date().toString()+listPrefix+" added "+pos);
                                listMemberCount = list.getMemberCount();  
                                waitCount = -1;
                            } else {
                                if (!hasCountUpdated(listMemberCount, list.getId()) && pos>=minPerCall){ 
                                 //   System.out.println(new Date().toString()+listPrefix+" waiting for "+pos);
                                 System.out.println("UNABLE TO BUILD LIST: "+adding);  
                                    
                                    waitCount = listMemberCount;                                    
                                }
                                return;
                                
                                
                            }
                        } catch (Exception e) {
                            if (e.getMessage().contains("page does not exist")) {
                                list = null;
                                createListIfNeeded();                        
                            } else   
                            if (e.getMessage().contains("overloaded")) {
                                try {
                                    Thread.sleep(backoff);
                                    if (backoff<1200000) {//20 minutes
                                        backoff *= 2;
                                    }
                                } catch (InterruptedException e1) {
                                    
                                    requestShutdown();
                                    return;
                                }
                            } else {
                                e.printStackTrace();
                                return;
                               // requestShutdown();
                              //  throw new RuntimeException(e);
                            }
                        }
                    } while(backoff>0);
    

                }
       
        }
    }

    private boolean hasCountUpdated(int originalCount, final long targetListId) throws TwitterException {
        boolean isUpdated = false;                     
        ResponseList<UserList> lists = twitter.getUserLists(listOwnerScreenName);
        int j = lists.size();
        while (--j>=0) {
            UserList list = lists.get(j);
            if (targetListId == list.getId()) {
                if (list.getMemberCount() > originalCount) {
                    isUpdated = true;
                    listMemberCount = list.getMemberCount();
                }
                break;
            }
        }
        return isUpdated;
    }

}
