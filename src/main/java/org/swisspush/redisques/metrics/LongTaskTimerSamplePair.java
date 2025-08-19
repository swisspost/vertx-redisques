package org.swisspush.redisques.metrics;

import io.micrometer.core.instrument.LongTaskTimer;
public class LongTaskTimerSamplePair {
    private LongTaskTimer longTaskTimer;
    private LongTaskTimer.Sample sample;


    public LongTaskTimerSamplePair(LongTaskTimer longTaskTimer, LongTaskTimer.Sample sample) {
        this.longTaskTimer = longTaskTimer;
        this.sample = sample;
    }

    public LongTaskTimer getLongTaskTimer() {
        return longTaskTimer;
    }

    public void setLongTaskTimer(LongTaskTimer longTaskTimer) {
        this.longTaskTimer = longTaskTimer;
    }

    public LongTaskTimer.Sample getSample() {
        return sample;
    }

    public void setSample(LongTaskTimer.Sample sample) {
        this.sample = sample;
    }

    @Override
    public String toString() {
        return "LongTaskTimerPair{" +
                "longTaskTimer=" + longTaskTimer +
                ", sample=" + sample +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LongTaskTimerSamplePair)) return false;
        LongTaskTimerSamplePair that = (LongTaskTimerSamplePair) o;
        return longTaskTimer.equals(that.longTaskTimer) &&
                sample.equals(that.sample);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(longTaskTimer, sample);
    }
}
