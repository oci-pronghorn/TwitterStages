package com.ociweb.pronghorn.adapter.twitter;

import java.util.Date;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.BloomFilter;
import com.ociweb.pronghorn.util.NOrdered;
import com.ociweb.pronghorn.util.TrieCollector;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;




public class WordFrequencyStage extends PronghornStage {
    
    
    static class LowerSequence implements CharSequence {
        private CharSequence cs;
        
        public void setValue(CharSequence cs) {
            this.cs = cs;
        }
        
        @Override
        public int length() {
            return cs.length();
        }

        @Override
        public char charAt(int index) {
           return Character.toLowerCase(cs.charAt(index));
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            LowerSequence local = new LowerSequence();
            local.setValue(cs.subSequence(start, end));
            return local;
        }
        
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(this);
            return builder.toString();
        }
        
    }
    
    private LowerSequence toLower = new LowerSequence();
    
    private Pipe<TwitterEventSchema> input;
    
    private TrieParser trie;
    private TrieParserReader reader;
    

    private final byte[] tweet = new byte[2000];
    
    private int floorCount = 10; //every word must be used this many times first before we consider it.

    private NOrdered<String> topWords = new NOrdered<String>(200);
    
    private TrieCollector collector = new TrieCollector(32*30_000);
    
   
    
    
    public WordFrequencyStage(GraphManager graphManager, Pipe<TwitterEventSchema> input) {
        super(graphManager, input, NONE);
        this.input = input;
        
        
    }
    
    @Override
    public void startup() {
        
        trie = new TrieParser(2048);
        TextContentRouterStage.extractWordPatterns(trie);
        
        reader = new TrieParserReader(1, 0, 2048);
    }

    
    
//    1 retry, did not add 
//    1 retry, did not add 
//    Invalid encoding, low byte must have bits of 10xxxxxx but we find 11111111111111111111111111100000
//    Invalid encoding, low byte must have bits of 10xxxxxx but we find 100000
//    Invalid encoding, low byte must have bits of 10xxxxxx but we find 11111111111111111111111111100000
//    Invalid encoding, low byte must have bits of 10xxxxxx but we find 100000
    
    int maxValue = 0;
    
    @Override
    public void run() {
        
        while (PipeReader.tryReadFragment(input)) {
            if (TwitterEventSchema.MSG_USERPOST_101 == PipeReader.getMsgIdx(input)) {
                
                
                int len = PipeReader.readBytes(input, TwitterEventSchema.MSG_USERPOST_101_FIELD_TEXT_22, tweet, 0);
                tweet[len]=32;
                int pos = 0;
                //split on 32
                //use trie with each ending in 32 or something new
                
                TrieParserReader.parseSetup(reader, tweet, pos, len+1, Integer.MAX_VALUE);
                
               // TrieParserReader.debugAsUTF8(reader, System.out);
                
                //is name
                //is noun
                //is verb
                //is adj
                
                
                //TODO: split words into Noise, Positive, Negative categories.
                
                
                //build prefillter of common words.
                String[] skipWords = new String[]{
                                                  "the","for","you","and","are","your","with","from","when","just","have","how","via","it's","you're","its","don't","I'm","too",
                                                  "that","will","can","make","this","what","new","than","was","over","life","like","who","all","off","big","use","say","which",
                                                  "check","not","but","out","now","why","after","one","don't","about","has","get","recent","never",
                                                  "want","need","our","they","time","earn","their","being","help","would","take","going","look","says","much","her","week","got",
                                                  "them","still","very","can't","back", "let", "may","looking","were",
                                                  "great","big","more","most","some","better","top", "love", "Ways","see","these","here","There","right","into",
                                                  "she","his","think","share","know","way","earn","good","only","Best","first","every","Day","should","really","always",
                                                  "man","through","does","someone","win","made","join","come","ever","gets","any","must","without","him",
                                                  "find","Please","There","never","Wife","Gets","Down","Into","Related","Extremely","Long","Hey","today","could","own",
                                                  "next","using","makes","change","latest","members","short","friend",
                                                  "talks","account","email","helping","country","there's","pieces","street","level","course",
                                                  "local", "often","moving","Retweets", "starts","dreams","Experts","States","Tweets", "positive",
                                                  "profitable","Laden","three", "Very","stories","paid","events","been",
                                                  "topic","Step","Leader", "guy", "against","Read","because","many","something","last","team","Tool","support",
                                                  "other","doing","Content","tell","Growth","getting","then","talk","Gala","yourself","show","Create","home",
                                                  "due","needs","where","give", "Increase","Connect", "tools","trade","Walking","daily",
                                                  "Small","post","happen","become",
                                                  "free", "energy", "money","making","work",
                                                  "There is","lead","not to","who will","piece","de la","St.","of you","trend","trend on","to let","will lead","critical",
                                                  "a free","event","wait","than a","There's","Dallas","free growth",
                                                  "to know","had","might","open","that's","on your","fun","is so","from a","up and","head","is that","have been", "Five","you need", "you need to",
                                                  "Living","truth","you do","and it","it will","keep yours","need to know","Away","Tomorrow","to know about",
                                                  "we can","wants","found","Hope","key","add","started","Easy","discuss","track","that will","so we","Feel","Hope you","so we can","continue","you all",
                                                  "plus","free to","choice","bet","main","to discuss", "out there", "to add", "continue","enjoyed","determine","it:","tracking","Feel free", "Feel free to",
                                                  "Kelly","I bet","add me","factors","to track","wars","government","show!","the success","got her","will determine","toaster","sheet","success of","can continue",
                                                  "hurt.","the scuccess of","enjoyed the","5 key","determine the","factors that","taxes","We found","can continue to","illegals","found it",
                                                  "Trump","Daughter","Sexy","try","Year","Super","stock","#RT","Probably","instead",
                                                  
                                                  "follow","follow!","#tcot","Security","idea","Nice","moment","Director",
                                                  
                                                  "you are", "days", "all the", "story", "Clinton", "to my", "to do", "part", "to your", "to all", "Reasons", "is to ", "with your",
                                                  "each", "election", "beautiful", "takes", "mother", "post on", "everything", "you", "heart", "State", "Training", "reach",
                                                  "eyes", "Galaxy", "#Trump", "What a", "least","lose","Instagram","true","cute","greatest", "under","to learn","Thanks","Amazing",
                                                  "should be","kind","perfect","What is","grow","Meet","make you","Have you","emotional",
                                                  
                                                  "Sales", "Facebook", "those","Money",
                                                  "online", "American", "vote",
                                                  "connect","world", "Tips",
                                                  "market","real","following","did","start","Obama","VIDEO!","it.","media","World","Young","money!","before","White","even","live",

                                                  "Hollywood!", "Jobs", "you.",  "Google",
                                                                                                    
                                                  "leaders","leader",
                                                  "Kingdom","USA",
                                                                                                    
                                                  "Quick", "Carbon",
                                                  
                                                  "people","I have","today!","trying","trying to","answer","used","leads","contact","cause","all your","address","over the","Around","tour","Hate",
                                                  "love.","used to","is too",
                                                  "code","sign","Friends","Uber","sign up","Use code","Get","in my",
                                                  "to us",
                                                  
//                                                  "Samsung", "Apple", "#Google", "YouTube","CEO",
//                                                  "Warren", "Christian","Auto","Ted","Shopify",
//                                                  "West","#stocks", "Rain","Performance",
//                                                  "Partner","Pro","podcast",
//                                                  "Ice","financial","#finance",
//                                                  "Radio","partner","promotes",
//                                                  "#health","Mom","George","#contentmarketing",
                                                  
                                                  "New post","World's","World's First","Help make","make it","Help make it",
                                                  
                                                  "tip","#wow","#wowjobs","job","@etsy","promo","advertisements",
                                                  "video","music","ads","God","Watch","Social","#SocialMedia",
                                                  "Online","Science","Season","Media","keep","Sanders","movie","Finally","Woman","Value","visit",
                                                  "thanks","#jobs","shop","news","@youtube","#quote","#news","#market","Star","Thank","Happy","Photo",
//                                                  "trump","2016","people","hillary","to:","#DMZ","...","#International","@realDonaldTrump","Cruz","#Worldwide",
//                                                   "????","YBG","Twitter","follow","#Twitter","#marketing","#TopNewFollowers","John","Amazon",
//                                                  
//                                                   //new values
//                                                  "#startup","#startups","#bigdata","data","#iot","#tech","business","internet","#business","Marketing",
//                                                  "#technology","#entrepreneur","Things","code","Learn","Tool Kit","#cloud","Entrepreneur","source","software",
//                                                  "platform","development","#security","solution","technology","analytics","entrepreneurs","product","Enterprise",
//                                                  "Innovation","Small Business","Startups","#Digital","modern","management","big data","Elon","Digital","IoT","future","#technews",
//                                                  "#leadership","Secure","hacking",
//                                                  "indicators",
//                                                  "political",
//                                                  "success",
//                                                  "#machinelearning","#bots","#chatbots",
                                                  
                                                  //NEW search words   #Tech,
                                                  
                                                  //wealthy words 
                                                  //equestrian  entrepreneurship  dermatologist  orthodontist escrow
                                                  
                                                  
                                                  "when you",
                                                  "for the",
                                                  "of the",
                                                  "want to",
                                                  "in the",
                                                  "how to",
                                                  "with a",
                                                  "you want",
                                                  "Do you",
                                                  "is a",
                                                  "on the",
                                                  "is the",
                                                  "is not",
                                                  "to the",
                                                  "to get",
                                                  "Check out",
                                                  "to be",
                                                  "have a",
                                                  "in your",
                                                  "a great",
                                                  "how to",
                                                  "with the",
                                                  "you want to",
                                                  "What Your",
                                                  "Related Articles:",
                                                  "a lot",
                                                  "How Long",
                                                  "This is",
                                                  "for a",
                                                  "lot of",
                                                  "from the",
                                                  "you to",
                                                  "if you",
                                                  "the best",
                                                  "in a",
                                                  "Thanks for",
                                                  "a lot of",
                                                  "you can",
                                                  "Thank you",
                                                  "Know About",
                                                  "will be",
                                                  "the most",
                                                  "have to",
                                                  "for being",
                                                  "this week",
                                                  "If you",
                                                  "Happy to",
                                                  "to connect",
                                                  "Happy to connect",
                                                  "SEE VIDEO!",
                                                  "out of",
                                                  "for your",
                                                  "you have",
                                                  "be a",
                                                  "and the",
                                                  "You Should",
                                                  "Social Media",
                                                  "of a",
                                                  "you need",
                                                  "The biggest",
                                                  "you have a",
                                                  "business that",
                                                  "do not",
                                                  "can help",
                                                  "That Are",
                                                  "Get your",
                                                  "out on",
                                                  "could be",
                                                  "How to Get",
                                                  "a business",
                                                  "who are",
                                                  "to our",
                                                  "money for",
                                                  "to work",
                                                  "that we",
                                                  "topic of",
                                                  "what you",
                                                  "up to",
                                                  "I am",
                                                  "Are you",
                                                  "going to",
                                                  "Vote for",
                                                  "Ways to",
                                                  "to help",
                                                  "to a",
                                                  "a new",
                                                  "to make",
                                                  "at the",
                                                  "need to",
                                                  "Thanks for the",
                                                  "have a great",
                                                  "it happen",
                                                  "make it happen",
                                                  "happen for",
                                                  
                                                  "Go to:"
                                                  
                                                  
                                                  
                };
                
                
                String previous2 = "";
                String previous = "";
                while (TrieParserReader.parseHasContent(reader)) {
                    long result = TrieParserReader.parseNext(reader, trie);
                    if (2==result) {// extracted word
                        
                        String v = TrieParserReader.capturedFieldBytesAsUTF8(reader, 0, new StringBuilder()).toString().replace('\n', ' ').replace('\r', ' ').trim();   
                                                                        
                        printRepeats(skipWords, v);
                        
                        if (!v.startsWith("@")) {//if starts with @ then its a twitter address.
                            toLower.setValue(v);
                           // System.out.print(toLower+"       has id of ");
                            int textId = collector.valueOfUTF8(toLower);             
                           // System.out.println(textId);
                            
                            if (textId>maxValue) {
                                maxValue = textId;
                                System.out.println("    words:" +textId+" "+toLower);
                            }
                        }
                        
                        if (previous.trim().length()>0) {
                            String v2 = previous+" "+v;
                                                        
                            printRepeats(skipWords, v2);     
                            if (previous2.trim().length()>0) {
                                String v3 = previous2+" "+v2;
                                                               
                                printRepeats(skipWords, v3);
                            }
                            
                        }
                        
                        previous2 = previous;
                        previous = v;
                        
                    }
                    if (3==result) {// extracted URL                        
                        
                    }
                    //all others ignored
                }
                
                
                
                
                //StringBuilder target = new StringBuilder();

                
                
                
                
//                //display to user.
//                String value = PipeReader.readUTF8(input, TwitterEventSchema.MSG_USERPOST_101_FIELD_SCREENNAME_53, target).toString();
//
//                if (!filter.mayContain(value)) {                
//                    
//                   // System.out.println(new Date()+" UNFOLLOW  http://twitter.com/"+value);                
//                    System.out.println(new Date()+" UNFOLLOW  https://twitter.com/intent/follow?screen_name="+value);
//                    
////                    try {
////                        //user interaction delay.
////                        Thread.sleep(1000+r.nextInt(3000));
////                        twitter.destroyFriendship(value);
////                        
////                        
////                    } catch (TwitterException e) {
////                        throw new RuntimeException(e);
////                    } catch (InterruptedException e) {
////                       
////                    }
//                    
//                    filter.addValue(value);
//                                        
//                }   
                
            }
            PipeReader.releaseReadLock(input);
        }

    }

    long nextPrintTime = 0;
    
    private void printRepeats(String[] skipWords, String v) {
        

        
        
        if (v.length()>2 && !v.toLowerCase().startsWith("htt")) {
        
            //filter skip words
            int j = skipWords.length;
            while (--j>=0) {
                if (skipWords[j].equalsIgnoreCase(v)) {
                    break;
                }
            }
            
            if (j<0) {
                topWords.sample(v);
                
                long now = System.currentTimeMillis();
                if (nextPrintTime<now) { 
                    System.out.println(new Date());
                    topWords.writeTo(System.out);
                    System.out.println();
                    System.out.println();
                    nextPrintTime = now+3600_000L; //once per hour
                }
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
