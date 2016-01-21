package com.ociweb.twitter;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.adapter.twitter.TwitterEventSchema;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class TwitterEventSchemaTest {

    
    @Test
    public void testFROMMatchesXML() {
        assertTrue(FROMValidation.testForMatchingFROMs("/com/ociweb/twitter/pronghorn/TwitterEvent.xml", TwitterEventSchema.instance));
    };
    
    @Test
    public void testConstantFields() { //too many unused constants.
        assertTrue(FROMValidation.testForMatchingLocators(TwitterEventSchema.instance));
    }
    
}
