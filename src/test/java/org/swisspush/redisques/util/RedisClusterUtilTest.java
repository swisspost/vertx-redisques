package org.swisspush.redisques.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class RedisClusterUtilTest {

    @Test
    public void testSameKeySameSlot() {
        int slot1 = RedisClusterUtil.redisSlot("mykey");
        int slot2 = RedisClusterUtil.redisSlot("mykey");

        assertEquals(slot1, slot2);
    }

    @Test
    public void testDifferentKeysDifferentSlots() {
        int slot1 = RedisClusterUtil.redisSlot("key1");
        int slot2 = RedisClusterUtil.redisSlot("key2");

        assertNotEquals(slot1, slot2);
    }

    @Test
    public void testHashTagSameSlot() {
        int slot1 = RedisClusterUtil.redisSlot("queue:{A}:1");
        int slot2 = RedisClusterUtil.redisSlot("queue:{A}:2");
        int slot3 = RedisClusterUtil.redisSlot("{A}");

        assertEquals(slot1, slot2);
        assertEquals(slot1, slot3);
    }

    @Test
    public void testHashTagDifferentSlots() {
        int slot1 = RedisClusterUtil.redisSlot("queue:{A}:1");
        int slot2 = RedisClusterUtil.redisSlot("queue:{B}:1");

        assertNotEquals(slot1, slot2);
    }

    @Test
    public void testNoClosingBraceUsesFullKey() {
        int slot1 = RedisClusterUtil.redisSlot("queue:{A");
        int slot2 = RedisClusterUtil.redisSlot("queue:{A");

        assertEquals(slot1, slot2);
    }

    @Test
    public void testEmptyHashTagIgnored() {
        int slot1 = RedisClusterUtil.redisSlot("queue:{}:1");
        int slot2 = RedisClusterUtil.redisSlot("queue:{}:2");

        // Empty {} should hash full key, so different
        assertNotEquals(slot1, slot2);
    }

    @Test
    public void testGetHashTagExtraction() {
        assertEquals("A", RedisClusterUtil.getHashTag("queue:{A}:1"));
        assertEquals("A", RedisClusterUtil.getHashTag("{A}"));
        assertEquals("key", RedisClusterUtil.getHashTag("key"));
    }

    @Test
    public void testSlotRange() {
        int slot = RedisClusterUtil.redisSlot("anykey");
        assertTrue(slot >= 0);
        assertTrue(slot < 16384);
    }
}