package com.ociweb.pronghorn.util.clustering;

import java.io.Serializable;

import com.ociweb.pronghorn.util.BloomFilter;

public class BitVectorsCollection implements Serializable {

    //TODO: add unit tests
    //TODO; only schedule the distance compute if the value as changed this is inside setValue, it must see if it was already set.
    //     we can only do 100+ per second so avoid doing any extras that are not needed.
    
    private final long[] data;
    private final int maxColumns;
    private final int bits;
    private final int maxLongs;
    
    //remove top and bottom ends,
    //all words that are common?
    //all words that only appear twice?

    //the maxFilters is about 1M, to do more we will use multiple instances of this class. and group users by frequency of use?
    
    public BitVectorsCollection(int maxRows, int maxColumns) {
        assert(maxRows>64);
    
        this.maxColumns = maxColumns;
        this.maxLongs = 2+(maxColumns>>6);//extra for meta data
        this.bits = 1+(int)(Math.ceil(Math.log(maxColumns)/Math.log(2)));
        
        long totalLongs = ((1+(maxLongs))*(long)(maxRows+1));
        assert(totalLongs <= Integer.MAX_VALUE);
        
        data = new long[(int)totalLongs];
        
    }
    
    public void setValue(int row, int column) {
        data[(row*(maxLongs))+(column>>6)] |=  (1L>>(0x03F&column));        
    }
    
    //store count of bits
    
    
    public static short distance(BitVectorsCollection that, int rowA, int rowB) {
        assert(rowB<rowA);
        return computeDistance(rowA*that.maxLongs, rowB*that.maxLongs, 0, that.maxLongs-1, that.data);
    }

    private static boolean isUnchanged(short value) {
        int a = 0x1F&value;
        int b = 0x1F&(value>>5);
        int c = 0x1F&(value>>10);
        return (a==b)&&(b==c);
    }
    
    public static int actualDistance(short value) {
        return 0x1F&(value>>5);
    }
    
    public static int getClearAbove(short value) {
        int a = 0x1F&value;
        int b = 0x1F&(value>>5);
        return a-b;
    }
    
    public static int getClearBelow(short value) {
        int b = 0x1F&(value>>5);
        int c = 0x1F&(value>>10);
        return b-c;
    }
    
    
    
    //return clear up to    clear above below methods?
    //return clear down to
    
    
    private static short computeDistance(int aIdx, int bIdx, int difTotal, int i, long[] localData) {
        
        //for each of these count bits changed since last time.
        //keep one int as the count of bits since last time.
        //recompute my count and check, recompute their count and compare.
        
        int totalIdx = i;
        int aTotal = 0;
        int bTotal = 0;
        while (--i>=0) {
            difTotal += Long.bitCount(localData[aIdx+i]^localData[bIdx+i]);
            aTotal += Long.bitCount(localData[aIdx+i]);
            bTotal += Long.bitCount(localData[bIdx+i]);
        }       
        
        int oldTotalA = (int)localData[aIdx+totalIdx];
        int oldTotalB = (int)localData[bIdx+totalIdx];
        
        int oldMaxBits = Math.max(oldTotalA, oldTotalB);        
        int newMaxBits = Math.max(aTotal, bTotal);
                
        int bound = Math.abs(aTotal-oldTotalA) + Math.abs(bTotal-oldTotalB);
                
        int distanceFloor  = (int)((((long)(difTotal-bound))<<16)/newMaxBits);//new is bigger we want floor to be lower
        int distance       = (int)((((long)difTotal)<<16)/newMaxBits);
        int distanceCeling = oldMaxBits==0?distance+1:(int)((((long)(difTotal+bound))<<16)/oldMaxBits);//old is smaller we want celing to be bigger
        
        //TODO work out new bounds arround this distance based on recent changes.
        
        int floorLeadingZeros  = Integer.numberOfLeadingZeros(distanceFloor);        
        int actualLeadingZeros = Integer.numberOfLeadingZeros(distance);
        int celingLeadingZeros = Integer.numberOfLeadingZeros(distanceCeling);        
        
        short result = (short) (((0x1F&celingLeadingZeros)<<10)|
                                ((0x1F&actualLeadingZeros)<<5)|
                                ((0x1F&floorLeadingZeros)));
                
        //if all 3 match we have nothing to update
        //else clear floor to ceiling and set actual.
        //return 5 bits high, low and actual? (work is in 32 distances)
        
        //TODO: this is a race conditions we must solve, now and specically when we have mutlple threads updating the graph.
        localData[aIdx+totalIdx] = aTotal;
        localData[bIdx+totalIdx] = bTotal;
        
        return result;
    }
    
    
    
}
