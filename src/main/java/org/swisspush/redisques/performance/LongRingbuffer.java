package org.swisspush.redisques.performance;

/**
 * <p>'long' in the class name refers to the type 'long'. It does NOT
 * say anything about how 'long' the buffer is!</p>
 */
public class LongRingbuffer {

    private final Object pushpopLock = new Object();
    private final long ring[];
    private int wrCur;
    private boolean isFilled;

    LongRingbuffer(int capacity) {
        if (capacity < 1) throw new IllegalArgumentException("assert(capacity >= 1)");
        this.ring = new long[capacity];
        this.wrCur = 0;
    }

    public void add(long value) {
        synchronized (pushpopLock) {
            //log.trace("ring[{}] = {}", wrCur, value);
            ring[wrCur] = value;
            wrCur += 1;
            if (wrCur >= ring.length) {
                wrCur = 0;
                isFilled = true;
            }
        }
    }

    public int read(long dst[], int off, int len) {
        synchronized (pushpopLock){
            int rangeOneOff, rangeOneLen, rangeTwoOff, rangeTwoLen;
            if (!isFilled) {
                rangeOneOff = 0;
                rangeOneLen = wrCur;
                rangeTwoOff = -999999;
                rangeTwoLen = 0;
            } else {
                rangeOneOff = wrCur;
                rangeOneLen = ring.length - rangeOneOff;
                rangeTwoOff = 0;
                rangeTwoLen = rangeOneOff == 0 ? 0 : wrCur;
            }
            int numCopied = 0;
            if( rangeOneLen > 0 ){
                //log.trace("readOne {}-{}", rangeOneOff, rangeOneOff + rangeOneLen);
                int numToCopy = Math.min(len, rangeOneLen);
                System.arraycopy(ring, rangeOneOff, dst, off, numToCopy);
                numCopied += numToCopy;
                len -= numToCopy;
                off += numToCopy;
            }
            if (rangeTwoLen > 0 && len > 0) {
                //log.trace("readTwo {}-{}", rangeTwoOff, rangeTwoOff + rangeTwoLen);
                int numToCopy = Math.min(len, rangeTwoLen);
                System.arraycopy(ring, rangeTwoOff, dst, off, numToCopy);
                numCopied += numToCopy;
                len -= numToCopy;
                off += numToCopy;
            }
            return numCopied;
        }
    }

}
