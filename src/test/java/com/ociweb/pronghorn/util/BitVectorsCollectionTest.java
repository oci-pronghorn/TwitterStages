package com.ociweb.pronghorn.util;

import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.util.clustering.BitVectorsCollection;

public class BitVectorsCollectionTest {

    @Ignore
    public void speedTest() {
        
        int rows = 30_000;
        int cols =  1000;  
        
        BitVectorsCollection bcv = new BitVectorsCollection(rows, cols);
        
        Random r = new Random(123);
        for(int i = 0; i<rows; i++) {
            
            for(int j = 0; j<cols*3; j++) {
                bcv.setValue(i, r.nextInt(cols));
            }
            
        }
        
        long start = System.currentTimeMillis();
        long sum = 0;
        for(int i = 0; i<rows; i++) {
            for(int j = 0; j< i /* 1 */; j++) { //28ms for 1 against 100K, it must be kept up to date at all times.
                                
                int distance = BitVectorsCollection.distance(bcv,i, j);
                
                sum +=distance;
               // System.out.println(Integer.toBinaryString(distance));
            }
        }
        long duration = System.currentTimeMillis()-start;
        
        System.out.println(sum+"  duration:"+duration);
        
    }
    
    //TODO: this is updated by a single thread be we need to support more in the future.
    //TODO: we only send update messages we a distance changes zones
    //TODO: messages should be very few and allow for more frequent tree building.
    
    
    
}
