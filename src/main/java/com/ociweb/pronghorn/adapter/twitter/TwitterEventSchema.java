package com.ociweb.pronghorn.adapter.twitter;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;

public class TwitterEventSchema extends MessageSchema{

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc040000e,0x80000000,0x90000000,0xac000000,0xac000001,0x80000001,0x80000002,0x80000003,0x90000001,0xac000002,0x80000004,0xac000003,0xac000004,0xac000005,0xc020000e,0xc0400010,0x80000000,0x90000000,0xac000000,0xac000001,0x80000001,0x80000002,0x80000003,0x90000001,0xac000002,0x80000004,0xac000003,0xac000004,0xac000005,0x90000002,0xac000006,0xc0200010},
            (short)0,
            new String[]{"User","Flags","UserId","Name","ScreenName","FavouritesCount","FollowersCount","FriendsCount","CreatedAt","Description","ListedCount","Language","TimeZone","Location",null,"UserPost","Flags","UserId","Name","ScreenName","FavouritesCount","FollowersCount","FriendsCount","CreatedAt","Description","ListedCount","Language","TimeZone","Location","PostId","Text",null},
            new long[]{100, 31, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 0, 101, 31, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 21, 22, 0},
            new String[]{"global",null,null,null,null,null,null,null,null,null,null,null,null,null,null,"global",null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null},
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
    
    
    public static final int MSG_USER_100 = 0x00000000;
    public static final int MSG_USER_100_FIELD_FLAGS_31 = 0x00000001;
    public static final int MSG_USER_100_FIELD_USERID_51 = 0x00800002;
    public static final int MSG_USER_100_FIELD_NAME_52 = 0x01600004;
    public static final int MSG_USER_100_FIELD_SCREENNAME_53 = 0x01600006;
    public static final int MSG_USER_100_FIELD_FAVOURITESCOUNT_54 = 0x00000008;
    public static final int MSG_USER_100_FIELD_FOLLOWERSCOUNT_55 = 0x00000009;
    public static final int MSG_USER_100_FIELD_FRIENDSCOUNT_56 = 0x0000000A;
    public static final int MSG_USER_100_FIELD_CREATEDAT_57 = 0x0080000B;
    public static final int MSG_USER_100_FIELD_DESCRIPTION_58 = 0x0160000D;
    public static final int MSG_USER_100_FIELD_LISTEDCOUNT_59 = 0x0000000F;
    public static final int MSG_USER_100_FIELD_LANGUAGE_60 = 0x01600010;
    public static final int MSG_USER_100_FIELD_TIMEZONE_61 = 0x01600012;
    public static final int MSG_USER_100_FIELD_LOCATION_62 = 0x01600014;
    public static final int MSG_USERPOST_101 = 0x0000000F;
    public static final int MSG_USERPOST_101_FIELD_FLAGS_31 = 0x00000001;
    public static final int MSG_USERPOST_101_FIELD_USERID_51 = 0x00800002;
    public static final int MSG_USERPOST_101_FIELD_NAME_52 = 0x01600004;
    public static final int MSG_USERPOST_101_FIELD_SCREENNAME_53 = 0x01600006;
    public static final int MSG_USERPOST_101_FIELD_FAVOURITESCOUNT_54 = 0x00000008;
    public static final int MSG_USERPOST_101_FIELD_FOLLOWERSCOUNT_55 = 0x00000009;
    public static final int MSG_USERPOST_101_FIELD_FRIENDSCOUNT_56 = 0x0000000A;
    public static final int MSG_USERPOST_101_FIELD_CREATEDAT_57 = 0x0080000B;
    public static final int MSG_USERPOST_101_FIELD_DESCRIPTION_58 = 0x0160000D;
    public static final int MSG_USERPOST_101_FIELD_LISTEDCOUNT_59 = 0x0000000F;
    public static final int MSG_USERPOST_101_FIELD_LANGUAGE_60 = 0x01600010;
    public static final int MSG_USERPOST_101_FIELD_TIMEZONE_61 = 0x01600012;
    public static final int MSG_USERPOST_101_FIELD_LOCATION_62 = 0x01600014;
    public static final int MSG_USERPOST_101_FIELD_POSTID_21 = 0x00800016;
    public static final int MSG_USERPOST_101_FIELD_TEXT_22 = 0x01600018;
    
    
    private TwitterEventSchema() {
        super(FROM);
    }
    
    
    public static final TwitterEventSchema instance = new TwitterEventSchema();
    
}
