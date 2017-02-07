package com.ociweb.pronghorn.adapter.twitter;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.BloomFilter;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

//TODO: make filter stage instead
public class TextContentRouterStage extends PronghornStage {

    
    
    private final Pipe<TwitterEventSchema> input;
    private final Pipe<TwitterEventSchema>[] output;
    
    private final byte[] tweet = new byte[2000];
    private final int fieldLoc;
    

    
    private TrieParser trie;
    private TrieParserReader reader;
    private BloomFilter[] filters;
    private boolean debug = false;
    

    
    public TextContentRouterStage(GraphManager graphManager, Pipe<TwitterEventSchema> input, Pipe<TwitterEventSchema>[] output, int fieldLoc, BloomFilter ... filters) {
        super(graphManager, input, output);
        this.input = input;
        this.output = output;
        this.fieldLoc = fieldLoc;
        this.filters = filters;
    }
    
    public TextContentRouterStage(boolean debug, GraphManager graphManager, Pipe<TwitterEventSchema> input, Pipe<TwitterEventSchema>[] output, int fieldLoc, BloomFilter ... filters) {
        super(graphManager, input, output);
        this.input = input;
        this.output = output;
        this.fieldLoc = fieldLoc;
        this.filters = filters;
        this.debug = debug;
    }
    
    
    @Override
    public void startup() {
        
        trie = new TrieParser(2048);
        TextContentRouterStage.extractWordPatterns(trie);
        
        reader = new TrieParserReader(2, 0, 2048);
  
    }
  
    private boolean moveInProgress = false;
    private int     moveTarget = -1;

    @Override
    public void run() {
        
        if (moveInProgress) {
            if (!PipeReader.tryMoveSingleMessage(input, output[moveTarget])) {
                return;
            } else {
                moveInProgress = false;
                moveTarget = -1;
                PipeReader.releaseReadLock(input);
            }
        }
        
        //TODO: can we pull images down then filter on the picutres
        //      fuzz picture and hash to bloom filter
        //      if seen before ...
        
        //TODO: must run on server 24/7 and accumulate lists till 500
        
        while (//PipeWriter.hasRoomForWrite(output[0]) && PipeWriter.hasRoomForWrite(output[1]) &&
               PipeReader.tryReadFragment(input) ) {

                int len = PipeReader.readBytes(input, fieldLoc, tweet, 0);
                tweet[len]=32;
                
//                String expected = PipeReader.readUTF8(input, fieldLoc, new StringBuilder()).toString();
//                String actual = new String(tweet,0,len);
//                if (!expected.equals(actual)) {
//                    System.err.println(expected);
//                    System.err.println(actual);
//                    
//                    throw new UnsupportedOperationException("no match");
//                }
//                
                
                
                int pos = 0;
                //split on 32
                //use trie with each ending in 32 or something new
                
                TrieParserReader.parseSetup(reader, tweet, pos, len+1, Integer.MAX_VALUE);
                
              //  TrieParserReader.debugAsUTF8(reader, System.err);
              //  System.err.println("NEXTTWEET");
                
                int hitWordCount = 0;
                
//                
//                StringBuilder target=new StringBuilder();
//                TrieParserReader.debugAsUTF8(reader,target);
                                
                String previous = "";

                while (TrieParserReader.parseHasContent(reader)) {
                    
                    
                    long result = TrieParserReader.parseNext(reader, trie);
                    if (-1==result) {
                        
                        System.err.println("unable to parse value abandoned, moving on to next");
                        PipeReader.releaseReadLock(input);
                        
                        return;
                        
//                        System.err.println(TrieParserReader.parseHasContentLength(reader)+" unable to parse ");
                        
//                        int p = TrieParserReader.debugAsUTF8(reader, System.err);
//                        System.err.println("BAD idx at "+p);
//                        requestShutdown();
//                        return;
                        
                    }
                    if (2==result) {
                        try{
                            String v = TrieParserReader.capturedFieldBytesAsUTF8(reader, 0, new StringBuilder()).toString().trim();
                         //   System.err.println("found word:"+v);
                            
                            int j = filters.length;
                            while (--j>=0) {
                                if (filters[j].mayContain(v) || filters[j].mayContain(previous+" "+v)) { //TODO: update may contain to take array of charSquence to avoid string construction.
                                    if (debug) {
                                        System.out.println("matched word found: "+v+" in filter "+j);
                                    }
                                    hitWordCount++;
                                }   
                            }
                            
                            previous = v;
                        } catch (Throwable t) {
                            System.err.println("dropped: unable to parse:"+t);
                            PipeReader.releaseReadLock(input);                            
                            return;
                        }
                    }
                    if (3==result || 4==result) {// extracted URL                        
                        
                    //    String v = TrieParserReader.capturedFieldBytesAsUTF8(reader, 0, new StringBuilder()).toString().trim();
                    //    System.err.println("found URL: "+v);
//                     
//                        //must check if this is a valid url
//                        
//                        try {
//                            URL url = new URL(3==result? "http://"+v : "https://"+v);
//                           
//                            //TODO: need bloom filter to avoid looking up same one twice.
//                            //then url goes into bad and good lists.
//                            
//                         //   System.out.println("URL: "+url);
//                            
//                        } catch (MalformedURLException e) {
//                            //we ignore malformed URLS
//                        }
                        
                        
                        
                        
                    }
                    //all others ignored
                }
                
            
                if (0==hitWordCount) {                        
                    
                    if (!PipeReader.tryMoveSingleMessage(input, output[0])) {
                        moveTarget = 0;
                        moveInProgress = true;
                        return;
                    }
                } else {
                    if (debug) {
                       // System.out.println("debugValue: "+target.toString().replace('\n',' ').replace('\r',' ')  );
                        
                        //write which account did this so we can confirm.
                        PipeReader.readUTF8(input, TwitterEventSchema.MSG_USERPOST_101_FIELD_SCREENNAME_53, System.out);
                        
                        
                    }
                    
                    if (!PipeReader.tryMoveSingleMessage(input, output[1])) {
                        moveTarget = 1;
                        moveInProgress = true;
                        return;
                    }
                }    
                PipeReader.releaseReadLock(input);

            
        }
        
    }


    public static void extractWordPatterns(TrieParser trie) {
        
        final int ignore = 1;
        final int word   = 2;

        trie.setUTF8Value("#",  ignore);  //Ignores
        trie.setUTF8Value(":",  ignore);  //Ignores
        trie.setUTF8Value(";",  ignore);  //Ignores
        trie.setUTF8Value(",",  ignore);  //Ignores
        trie.setUTF8Value("!",  ignore);  //Ignores
        trie.setUTF8Value("?",  ignore);  //Ignores
        trie.setUTF8Value("\\", ignore);  //Ignores
        trie.setUTF8Value("/",  ignore);  //Ignores
        trie.setUTF8Value(" ",  ignore);  //Ignores
        trie.setUTF8Value("\"", ignore);  //Ignores
        trie.setUTF8Value(" ",  ignore);  //Ignores
        trie.setUTF8Value("'",  ignore);  //Ignores
        trie.setUTF8Value("&",  ignore);  //Ignores
        trie.setUTF8Value("-",  ignore);  //Ignores
        trie.setUTF8Value("+",  ignore);  //Ignores
        trie.setUTF8Value("|",  ignore);  //Ignores
        trie.setUTF8Value(">",  ignore);  //Ignores
        trie.setUTF8Value("_",  ignore);  //Ignores
        trie.setUTF8Value("^",  ignore);  //Ignores
        trie.setUTF8Value(".",  ignore);  //Ignores
        trie.setUTF8Value(")",  ignore);  //Ignores
        trie.setUTF8Value("<",  ignore);  //Ignores
        trie.setUTF8Value("[",  ignore);  //Ignores
        trie.setUTF8Value("]",  ignore);  //Ignores
        trie.setUTF8Value("$",  ignore);  //Ignores
        trie.setUTF8Value("~",  ignore);  //Ignores
        
        //DO NOT ADD : OR // SINCE THAT WILL BLOCK URLS
        trie.setUTF8Value("%b?",  word); //new word
        trie.setUTF8Value("%b\"", word); //new word
        trie.setUTF8Value("%b ",  word); //new word
        trie.setUTF8Value("%b.",  word); //new word
        trie.setUTF8Value("%b,",  word); //new word
        trie.setUTF8Value("%b!",  word); //new word
        trie.setUTF8Value("%b:",  word); //new word        
        trie.setUTF8Value("%b(",  word); //new word
        trie.setUTF8Value("%b)",  word); //new word
        trie.setUTF8Value("%b+",  word); //new word
        trie.setUTF8Value("%b-",  word); //new word
        trie.setUTF8Value("%b_",  word); //new word
        trie.setUTF8Value("%b[",  word); //new word
        trie.setUTF8Value("%b]",  word); //new word
        trie.setUTF8Value("%b{",  word); //new word
        trie.setUTF8Value("%b}",  word); //new word
        
        trie.setUTF8Value("https://%b " ,4);//URL 
        trie.setUTF8Value("http://%b " ,3);//URL 
        
        
    }

}
