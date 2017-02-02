package com.ociweb.pronghorn.adapter.twitter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.BloomFilter;


/*
 * data passes through this stage twice.
 * first pass to filter messages
 * second pass to record new ids to add to the filter
 * 
 */


//TODO: promote to utiltilty stage for general use.
public class PassUniquesFilterTwoStepStage<T extends MessageSchema> extends PronghornStage {

    //after instances of each id then let them pass
    //saves its own state
    
    private final int maximumItems = 10_000_000;
    private final double maximumFailure = .00001; 
    // 10_000_000  .00001 -> 32MB
    
    private BloomFilter filter;
    private final Pipe<T> input;
    private final Pipe<T> output;
    private final Pipe<RawDataSchema> addFilterPipe;
    
    private final int varFieldLoc;
    private final File storage;
    private final File backup;
    private boolean moveInProgress = false;            
    
    public PassUniquesFilterTwoStepStage(GraphManager graphManager, Pipe<T> input, Pipe<RawDataSchema> addFilterPipe, Pipe<T> output, int varFieldLoc, File storage) {
        super(graphManager, new Pipe[] {input, addFilterPipe}, output);
        this.input = input;
        this.output = output;
        this.addFilterPipe = addFilterPipe;

        this.varFieldLoc = varFieldLoc;
        this.storage = storage;
        try {
            this.backup = new File(storage.getCanonicalPath()+".bak");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }

    @Override
    public void startup() {
   
        if (storage.exists()) {
            File localStorage = storage;
            
            try {
                filter = loadFilter(localStorage);
            } catch (Exception e) {
                System.out.println("Unable to load old filters, starting with new files");
                buildNewFilters();
            }
            
            System.out.println("loaded BloomFilter");
            System.out.println("seen pct full "+ (100f*filter.pctConsumed()));
            System.out.println();            
            
        } else {
            buildNewFilters();
        }
    }

    private BloomFilter loadFilter(File file) throws FileNotFoundException, IOException, ClassNotFoundException {
        FileInputStream fist = new FileInputStream(file);
        ObjectInputStream oist = new ObjectInputStream(fist);                
        BloomFilter localFilter = (BloomFilter) oist.readObject();
         
        oist.close();
        return localFilter;
    }

    private void buildNewFilters() {
        filter = new BloomFilter(maximumItems, maximumFailure);
    }
    
    @Override
    public void shutdown() {
        //NOTE: this save resume logic is all new and may be replaced with pipes and/or and external system.
        
        saveFilter();
    }

    private void saveFilter() {
        try {
            if (backup.exists()) {
                backup.delete();
            }
            storage.renameTo(backup);
            
            FileOutputStream fost = new FileOutputStream(storage);
            ObjectOutputStream oost = new ObjectOutputStream(fost);
            oost.writeObject(filter);
            oost.close();
            
        } catch (Exception e) {
           throw new RuntimeException(e);
        }
    }

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
        
        boolean updateFile = false;
  
        while (PipeReader.tryReadFragment(addFilterPipe)) {
            byte[] backing = PipeReader.readBytesBackingArray(addFilterPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
            int pos = PipeReader.readBytesPosition(addFilterPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
            int len = PipeReader.readBytesLength(addFilterPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);
            int mask = PipeReader.readBytesMask(addFilterPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2);                                 
            
            if (!filter.mayContain(backing,pos,len,mask)) {
 
                System.out.print("add followed: ");
                Appendables.appendUTF8(System.out, backing, pos, len, mask);
                System.out.println();          
                
                updateFile = true;
                filter.addValue(backing,pos,len,mask);
            }
            
            PipeReader.releaseReadLock(addFilterPipe);
        }                
        
        while (PipeWriter.hasRoomForWrite(output) && PipeReader.tryReadFragment(input)) {
            byte[] backing = PipeReader.readBytesBackingArray(input, varFieldLoc);
            int pos = PipeReader.readBytesPosition(input, varFieldLoc);
            int len = PipeReader.readBytesLength(input, varFieldLoc);
            int mask = PipeReader.readBytesMask(input, varFieldLoc);
                        
            if (!filter.mayContain(backing,pos,len,mask)) {
                if (! PipeReader.tryMoveSingleMessage(input, output)) {
                    moveInProgress = true;
                    return;
                }                
            }
            
            PipeReader.releaseReadLock(input);
        }
        if (updateFile) {
            saveFilter();
        }
    }   
    

}
