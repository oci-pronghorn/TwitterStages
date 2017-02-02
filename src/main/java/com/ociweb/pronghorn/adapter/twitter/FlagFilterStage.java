package com.ociweb.pronghorn.adapter.twitter;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

//TODO: promote to utiltilty stage for general use.
public class FlagFilterStage<T extends MessageSchema> extends PronghornStage {

    //after instances of each id then let them pass
    //saves its own state
    
    
    private final Pipe<T> input;
    private final Pipe<T> output;
    private final int varFieldLoc;
    private final int mask;
    private int count;
    
    public FlagFilterStage(GraphManager graphManager, Pipe<T> input, Pipe<T> output, int varFieldLoc, int mask) {
        super(graphManager, input, output);
        this.input = input;
        this.output = output;
        this.varFieldLoc = varFieldLoc;
        this.mask = mask;
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
            
            if (0 != (mask&value)) {
                if (!PipeReader.tryMoveSingleMessage(input, output)) {
                    moveInProgress = true;
                    return;
                }              
            }
            PipeReader.releaseReadLock(input);
        }        
    }    

}
