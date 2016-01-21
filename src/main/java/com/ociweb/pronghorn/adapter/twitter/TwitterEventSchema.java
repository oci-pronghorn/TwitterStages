package com.ociweb.pronghorn.adapter.twitter;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;

public class TwitterEventSchema extends MessageSchema{

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc040000c,0x80000000,0x90000000,0x80000001,0x80000002,0xac000000,0xac000001,0xac000002,0xac000003,0xac000004,0xac000005,0xac000006,0xc020000c},
            (short)0,
            new String[]{"Event","Flags","Id","FollowersCount","FollowingCount","Name","ScreenName","Description","Location","Language","TimeZone","Text",null},
            new long[]{3, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 0},
            new String[]{"global",null,null,null,null,null,null,null,null,null,null,null,null},
            "TwitterEvent.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});
    
        
    public static final int FLAG_POSSIBLY_SENSITIVE       = 0b00000000_00000001;
    public static final int FLAG_FAVORITED                = 0b00000000_00000010;
    public static final int FLAG_RETWEET                  = 0b00000000_00000100;
    public static final int FLAG_RETWEETED                = 0b00000000_00001000;
    public static final int FLAG_RETWEETED_BY_ME          = 0b00000000_00010000;
    public static final int FLAG_TRUNCATED                = 0b00000000_00100000;
    
    public static final int FLAG_USER_PROTECTED           = 0b10000000_00000000;
    public static final int FLAG_USER_VERIFIED            = 0b01000000_00000000;
    public static final int FLAG_USER_FOLLOW_REQUEST_SENT = 0b00100000_00000000;
    public static final int FLAG_USER_GEO_ENABLED         = 0b00010000_00000000;
    
    
    public static final int MSG_EVENT_3 = 0x00000000;
    public static final int MSG_EVENT_3_FIELD_FLAGS_31 = 0x00000001;
    public static final int MSG_EVENT_3_FIELD_ID_32 = 0x00800002;
    public static final int MSG_EVENT_3_FIELD_FOLLOWERSCOUNT_33 = 0x00000004;
    public static final int MSG_EVENT_3_FIELD_FOLLOWINGCOUNT_34 = 0x00000005;
    public static final int MSG_EVENT_3_FIELD_NAME_35 = 0x01600006;
    public static final int MSG_EVENT_3_FIELD_SCREENNAME_36 = 0x01600008;
    public static final int MSG_EVENT_3_FIELD_DESCRIPTION_37 = 0x0160000A;
    public static final int MSG_EVENT_3_FIELD_LOCATION_38 = 0x0160000C;
    public static final int MSG_EVENT_3_FIELD_LANGUAGE_39 = 0x0160000E;
    public static final int MSG_EVENT_3_FIELD_TIMEZONE_40 = 0x01600010;
    public static final int MSG_EVENT_3_FIELD_TEXT_41 = 0x01600012;
    
    
    private TwitterEventSchema() {
        super(FROM);
    }
    
    
    public static final TwitterEventSchema instance = new TwitterEventSchema();
    
}
