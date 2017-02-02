package com.ociweb.pronghorn.util.clustering;

import com.ociweb.pronghorn.util.BloomFilter;
import com.ociweb.pronghorn.util.BloomFilterUtil;

public class HCNode {

    private final BloomFilter bloomFilter; //as leaf
    private final HCNode left;
    private final HCNode right;
    
    private HCNode next;
    
    public HCNode(BloomFilter bloomFilter, HCNode next) {
       this.next = next;
       
       this.bloomFilter = bloomFilter;
       this.left = null;
       this.right = null;
    }
    
    public HCNode(HCNode left, HCNode right, HCNode next) {
        this.next = next;
        //TOOD: build new intersection bloom filter for left and right to be captured.
        
        this.bloomFilter = null;
        this.left = left;
        this.right = right;
    }
    
    //TODO: rewrite distance to simplify and move down one tree? is this needed if we have nearest?
    public int distance(HCNode that) {
        
        if (null == this.bloomFilter) {
            if (null == that.bloomFilter) {
                //cluster to cluster
                //use cluster against all children to find the smallest.
                return this.shortestDistance(that.bloomFilter);
            } else {
                //use that against all children to find smallest
                return this.shortestDistance(that);
            }            
        } else {
            if (null == that.bloomFilter) {
                //use this against all children to find the smallest
                return that.shortestDistance(this.bloomFilter);
            } else {
                return BloomFilterUtil.simpleDistance(this.bloomFilter, that.bloomFilter);
            }            
        }
    }
    
    private int shortestDistance(HCNode node) {
        if (null!=bloomFilter) {
            return node.shortestDistance(bloomFilter);            
        } else {
            if (null!=node.bloomFilter) {
                return this.shortestDistance(node.bloomFilter);
            } else {
                int leftValue = this.left.shortestDistance(node);
                int rightValue = this.right.shortestDistance(node);
                return Math.min(leftValue, rightValue);
            }
        }
    }
    
    private int shortestDistance(BloomFilter filter) {
        if (null!=bloomFilter) {
            return BloomFilterUtil.simpleDistance(bloomFilter, filter);
        } else {
            int leftValue = left.shortestDistance(filter);
            int rightValue = right .shortestDistance(filter);
            return Math.min(leftValue, rightValue);
        }
    }

}
