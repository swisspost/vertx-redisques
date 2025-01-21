package org.swisspush.redisques.util;

public class DebugInfo {

    public static String __WHERE__() { return __WHERE__(1); }

    public static String __WHERE__(int numFramesToIgnore) {
        var frame = Thread.currentThread().getStackTrace()[2 + numFramesToIgnore];
        return frame.getClassName() + "." + frame.getMethodName() + "():L" + frame.getLineNumber();
    }

}
