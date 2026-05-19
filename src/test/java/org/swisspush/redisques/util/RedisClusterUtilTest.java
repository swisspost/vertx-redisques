package org.swisspush.redisques.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.swisspush.redisques.util.RedisClusterUtil.groupKeysBySlot;
import static org.swisspush.redisques.util.RedisClusterUtil.orderKeysBySlot;
import static org.swisspush.redisques.util.RedisClusterUtil.calcRedisSlot;

public class RedisClusterUtilTest {

    @Test
    public void testOneMillionSpeed() {
        List<String> keys = new ArrayList<>();

        for (int i = 0; i < 1_000_000; i++) {
            keys.add("queue:{user" + i + "}:data:verylong:key:abcdefghigklmnopqrstuvwxyz");
        }

        long start = System.nanoTime();
        long sum = 0;
        for (String key : keys) {
            sum += calcRedisSlot(key);
        }
        long end = System.nanoTime();
        System.out.println("slots hashing time usage ms: " + (end - start) / 1_000_000.0);

        start = System.nanoTime();
        orderKeysBySlot(keys);
        end = System.nanoTime();
        System.out.println("order keys by slot time usage ms: " + (end - start) / 1_000_000.0);

        start = System.nanoTime();
        groupKeysBySlot(keys);
        end = System.nanoTime();
        System.out.println("group keys by slot time usage ms: " + (end - start) / 1_000_000.0);
    }

    @Test
    public void testSameKeySameSlot() {
        int slot1 = calcRedisSlot("mykey");
        int slot2 = calcRedisSlot("mykey");

        assertEquals(slot1, slot2);
    }

    @Test
    public void testDifferentKeysDifferentSlots() {
        int slot1 = calcRedisSlot("key1");
        int slot2 = calcRedisSlot("key2");

        assertNotEquals(slot1, slot2);
    }

    @Test
    public void testHashTagSameSlot() {
        int slot1 = calcRedisSlot("queue:{A}:1");
        int slot2 = calcRedisSlot("queue:{A}:2");
        int slot3 = calcRedisSlot("{A}");

        assertEquals(slot1, slot2);
        assertEquals(slot1, slot3);
    }

    @Test
    public void testHashTagDifferentSlots() {
        int slot1 = calcRedisSlot("queue:{A}:1");
        int slot2 = calcRedisSlot("queue:{B}:1");

        assertNotEquals(slot1, slot2);
    }

    @Test
    public void testNoClosingBraceUsesFullKey() {
        int slot1 = calcRedisSlot("queue:{A");
        int slot2 = calcRedisSlot("queue:{A");

        assertEquals(slot1, slot2);
    }

    @Test
    public void testEmptyHashTagIgnored() {
        int slot1 = calcRedisSlot("queue:{}:1");
        int slot2 = calcRedisSlot("queue:{}:2");

        // Empty {} should hash full key, so different
        assertNotEquals(slot1, slot2);
    }

    @Test
    public void testSlotRange() {
        int slot = calcRedisSlot("anykey");
        assertTrue(slot >= 0);
        assertTrue(slot < 16384);
    }

    @Test
    public void testSameSlotKeysStayTogether() {
        List<String> keys = List.of(
                "queue:{A}:1",
                "queue:{A}:2",
                "queue:{A}:3"
        );

        List<String> ordered = RedisClusterUtil.orderKeysBySlot(keys);

        // All keys same slot → order should remain same
        assertEquals(keys, ordered);
    }

    @Test
    public void testDifferentSlotsGrouped() {
        List<String> keys = List.of(
                "queue:{B}:1",
                "queue:{A}:1",
                "queue:{C}:1",
                "queue:{A}:2"
        );

        List<String> ordered = RedisClusterUtil.orderKeysBySlot(keys);

        // Keys with same slot should be adjacent
        int slotA = calcRedisSlot("queue:{A}:1");

        int firstA = -1;
        int lastA = -1;

        for (int i = 0; i < ordered.size(); i++) {
            if (calcRedisSlot(ordered.get(i)) == slotA) {
                if (firstA == -1) firstA = i;
                lastA = i;
            }
        }

        // All A keys should be consecutive
        assertEquals(2, lastA - firstA + 1);
    }

    @Test
    public void testSlotOrderAscending() {
        List<String> keys = List.of(
                "queue:{C}:1",
                "queue:{A}:1",
                "queue:{B}:1"
        );

        List<String> ordered = RedisClusterUtil.orderKeysBySlot(keys);

        int prevSlot = -1;
        for (String key : ordered) {
            int slot = calcRedisSlot(key);
            assertTrue(slot >= prevSlot);
            prevSlot = slot;
        }
    }

    @Test
    public void testEmptyInput() {
        List<String> keys = List.of();

        List<String> ordered = RedisClusterUtil.orderKeysBySlot(keys);

        assertTrue(ordered.isEmpty());
    }

    @Test
    public void testSingleKey() {
        List<String> keys = List.of("queue:{A}:1");

        List<String> ordered = RedisClusterUtil.orderKeysBySlot(keys);

        assertEquals(1, ordered.size());
        assertEquals("queue:{A}:1", ordered.get(0));
    }

    @Test
    public void testOrderPreservedWithinSameSlot() {
        List<String> keys = List.of(
                "queue:{A}:1",
                "queue:{A}:2",
                "queue:{A}:3"
        );

        List<String> ordered = RedisClusterUtil.orderKeysBySlot(keys);

        assertEquals("queue:{A}:1", ordered.get(0));
        assertEquals("queue:{A}:2", ordered.get(1));
        assertEquals("queue:{A}:3", ordered.get(2));
    }

    @Test
    public void testRedisOfficialHashTagExample() {
        int slot1 = calcRedisSlot("{user1000}.following");
        int slot2 = calcRedisSlot("{user1000}.followers");

        assertEquals(slot1, slot2);
    }
}