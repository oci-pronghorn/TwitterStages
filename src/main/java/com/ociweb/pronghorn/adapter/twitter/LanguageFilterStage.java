package com.ociweb.pronghorn.adapter.twitter;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

//TODO: make filter stage instead
public class LanguageFilterStage extends PronghornStage {

    
    
    private final Pipe<TwitterEventSchema> input;
    private final Pipe<TwitterEventSchema>[] output;
    
    private final byte[] tweet = new byte[2000];
    private final int fieldLoc;
    
    private TrieParser trie;
    private TrieParserReader reader;
    
    
    //TODO: urgent hosebird find everyone tweeting on Java and other related subjects, keep list sorted by most followers, make list of these to be followed urgent
    
//find Spanish and french as well.
    
//    expected non-zero 0
//    found hit at 125 in RT @WalshFreedom: Saturday Night Live mocks Christians again.
//
//    Hey #SNL, until you have the courage to mock Muslims, shut the fuck up. http…
//    expected non-zero 0
//    found hit at 14 in Jennifer Lopez Sex Tape To Be Released By Ex https://t.co/xeOcy2F70x
//    expected non-zero 0
//    F: '❤️❤️❤️' [10084, 65039, 10084, 65039, 10084, 65039]
//    foreign take the 16 oz challenge today and reward yourself with silky smooth skin ❤️❤️❤️ https://t.co/XCxyshnTUV
//    expected non-zero 0
//    found hit at 12 in Electronic “Tattoos” for Healthcare Shift to Gather Business Data https://t.co/u6k3H9LmSe
//    expected non-zero 0
    
    public LanguageFilterStage(GraphManager graphManager, Pipe<TwitterEventSchema> input, Pipe<TwitterEventSchema>[] output, int fieldLoc) {
        super(graphManager, input, output);
        this.input = input;
        this.output = output;
        this.fieldLoc = fieldLoc;
    }
    
    
    @Override
    public void startup() {
        
        trie = new TrieParser(2048);
        TextContentRouterStage.extractWordPatterns(trie);
        
        reader = new TrieParserReader(1, 0, 2048);
  
    }
  
    String[] zones = {"Rome", "Santiago", "Brasilia", "Buenos Aires", "Quito", "Athens" ,"Bogota", "Warsaw", "Tokyo", "Lima", "Mazatlan", "Central America","Caracas",
            "Jakarta","Casablanca","Hong Kong","Yakutsk","Taipei","La Paz","Kuala Lumpur","Sapporo","Seoul","Solomon Is.","Osaka","Nuku'alofa","Budapest","Volgograd",
            "Minsk","Guam","Bratislava","Georgetown","Riga","Zagreb","Novosibirsk","Ekaterinburg","Krasnoyarsk","St. Petersburg","Cape Verde Is.","Marshall Is.",
           "Chennai","Baghdad","Bangkok","Baghdad","Bucharest","Beijing","Jerusalem","New Delhi","Ljubljana","Irkutsk","Istanbul","Belgrade","Singapore",
           "Cairo","Kyiv","Riyadh","Mumbai","Africa","Karachi","Abu Dhabi","Moscow","Sarajevo","Abu Dhabi","Nairobi","Kolkata","Islamabad","Almaty","Yerevan","Skopje",
           "Tehran","Sofia","Monrovia","Pretoria","Tbilisi","Asia","Kathmandu","Harare","Urumqi","Baku","Muscat","Hanoi","Kuwait","Volgograd","Sri Jayawardenepura",
           "Ulaan Bataar","Kabul","Astana"};
    
    
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
        
        while (PipeWriter.hasRoomForWrite(output[0]) && PipeWriter.hasRoomForWrite(output[1]) &&
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
                int foreignWords = 0;
                
                //TODO: refacor to its own stage
                String lang = PipeReader.readUTF8(input, TwitterEventSchema.MSG_USER_100_FIELD_LANGUAGE_60, new StringBuilder()).toString().toLowerCase();
                if ("spanish".equals(lang) || "french".equals(lang) || "portuguese".equals(lang)) {
                    foreignWords++;
                }
                
                
                String zone = PipeReader.readUTF8(input, TwitterEventSchema.MSG_USER_100_FIELD_TIMEZONE_61, new StringBuilder()).toString().toLowerCase();
                int z = zones.length;
                while (--z>=0) {
                    if (zone.equalsIgnoreCase(zones[z])) {
                        foreignWords++;
                    }
                }            
                
                
                
                
       //         StringBuilder target=new StringBuilder();
        //        TrieParserReader.debugAsUTF8(reader,target);
                                
                
                while (TrieParserReader.parseHasContent(reader)) {
                    long result = TrieParserReader.parseNext(reader, trie);
                    if (2==result) {
                        
                        String v = TrieParserReader.capturedFieldBytesAsUTF8(reader, 0, new StringBuilder()).toString().trim();
                        
                        //TODO: make this a different stage to find foreing words 
                        int i = v.length();
                        if (i >=4) {
                            int odd = 0; 
                            int[] data = new int[i];
                            while (--i >= 0) {
                                int c = v.charAt(i);
                                data[i]=c;
                                if ((c<0 || c>512) &&
                                        (c!=10084 && c!=65039) //heart is NOT a foreign char
                                    ) {
                                    odd++;
                                }
                            }
                            if (odd > (v.length()-(v.length()>>2))) {
                              //  System.out.println("ForeignWordFound : '"+v+"' "+Arrays.toString(data));    
                                foreignWords++;
                            }
                        }
                        
                    }
                    if (3==result) {// extracted URL                        
                        
                    }
                    //all others ignored
                }
                

                if (0==foreignWords) {                         
                    
                    if (!PipeReader.tryMoveSingleMessage(input, output[0])) {
                        moveTarget = 0;
                        moveInProgress = true;
                        return;
                    }
                } else {
                    
                 //   System.out.println("debugValue:"+target.toString().replace('\n',' ').replace('\r',' ')  );
                    
                    
                    if (!PipeReader.tryMoveSingleMessage(input, output[1])) {
                        moveTarget = 1;
                        moveInProgress = true;
                        return;
                    }
                }    

                PipeReader.releaseReadLock(input);

            
        }
        
    }

}
