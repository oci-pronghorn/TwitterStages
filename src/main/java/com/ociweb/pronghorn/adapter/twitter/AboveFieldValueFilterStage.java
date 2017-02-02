package com.ociweb.pronghorn.adapter.twitter;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

//TODO: promote to utiltilty stage for general use.
public class AboveFieldValueFilterStage<T extends MessageSchema> extends PronghornStage {

    //after instances of each id then let them pass
    //saves its own state
    
    
    private final Pipe<T> input;
    private final Pipe<T> output;
    private final int varFieldLoc;
    private final int varFieldLoc2;
    private final int followersLimit;
    private final int followingLimit;
    private int count;
    
    public AboveFieldValueFilterStage(GraphManager graphManager, Pipe<T> input, Pipe<T> output, int varFieldLoc, int varFieldLoc2, int limit) {
        super(graphManager, input, output);
        this.input = input;
        this.output = output;
        this.varFieldLoc = varFieldLoc;
        this.varFieldLoc2 = varFieldLoc2;
        this.followersLimit = limit;
        this.followingLimit = 7*(limit/8);//at least 25% of other number
    }

    @Override
    public void startup() {
    }

    
    @Override
    public void shutdown() {
    }
    
    boolean moveInProgress = false;
    
    
    @Override
    public void run() {
        
        if (moveInProgress) {
            if (!PipeReader.tryMoveSingleMessage(input, output)) {
                return;
            } else {
                moveInProgress = false;
                PipeReader.releaseReadLock(input);
            }
        }
        
        while (PipeWriter.hasRoomForWrite(output) && PipeReader.tryReadFragment(input)) {
                        
            count++;
            int value = PipeReader.readInt(input, varFieldLoc);   //following
            int value2 = PipeReader.readInt(input, varFieldLoc2); //followers
                        
            if (value>followingLimit && value2>followersLimit && value<value2) {
                if (!PipeReader.tryMoveSingleMessage(input, output)) {
                    moveInProgress = true;
                    return;
                }              
            }
            PipeReader.releaseReadLock(input);
        }
        
    }   
    

}
