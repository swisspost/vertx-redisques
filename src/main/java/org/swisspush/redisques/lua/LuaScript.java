package org.swisspush.redisques.lua;

public enum LuaScript {
    CHECK("redisques_check.lua");

    private String file;

    LuaScript(String file) {
        this.file = file;
    }

    public String getFile() {
        return file;
    }
}
