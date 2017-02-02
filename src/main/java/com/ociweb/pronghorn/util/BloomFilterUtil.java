package com.ociweb.pronghorn.util;

public class BloomFilterUtil {


    //TODO: add method for adding int fields to bloom filter
    
    //TODO: when two filters are similar we can take an intersection, if we have the orignal fields we can ask against it what the % weighting ended up as !!!

    //TODO: build method for measuring a new hash and its overlap percentage.
    //      start with cluster center and empty filter
    //      add each attribute to new filter and compare values on each round
    //      we know how many bits are added (one per hash), we know how many are outside ( will not work becaue of previous bits outside)
    //      requires new filter for each round and this could be messy,  add new method to bloom. 
    //      like mayContain except when it does not return count of bits mismatched.!!
    
    
    
    /**
     * Distance between two bloom filters. 
     * @param a this filter
     * @param b that filter
     * @return value between 0 and 1<<30 inclusive
     */
    public static int simpleDistance(BloomFilter a, BloomFilter b) {
        assert(a.bloomSize==b.bloomSize) : "Can not compare dissimilar sized filters";
        assert(a.isMatchingSeeds(b)) : "Can not compare dissimilar seeded filters";
        
        long thisCount       = 0;
        long thatCount       = 0;
        long thisOrThatCount = 0;
        
        int j = a.bloomSize;
        while (--j>=0) {
            
            long thisValue = BloomFilter.getDataAtIndex(a,j);
            long thatValue = BloomFilter.getDataAtIndex(b,j);
            
            thisCount += (long)Long.bitCount(thisValue);
            thatCount += (long)Long.bitCount(thatValue);
                        
            long union = thisValue | thatValue;
            thisOrThatCount += (long)Long.bitCount(union);

        }
        
        long max = thisCount+thatCount;
        long min = Math.max(thisCount, thatCount);
        long range = max-min;
        assert(thisOrThatCount<=range);
        assert(thisOrThatCount>=0);
        assert(thisOrThatCount < (1<<30));        
        
        return (int)((  thisOrThatCount<<30)/range);
    }
    
}
