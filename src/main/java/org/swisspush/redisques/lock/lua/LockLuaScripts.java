package org.swisspush.redisques.lock.lua;

import org.swisspush.redisques.lua.LuaScript;

/**
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public enum LockLuaScripts implements LuaScript {

    LOCK_RELEASE("lock_release.lua");

    private String file;

    LockLuaScripts(String file) {
        this.file = file;
    }

    @Override
    public String getFilename() {
        return file;
    }
}
