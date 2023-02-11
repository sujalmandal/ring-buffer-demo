package sujal.mandal.learn;

import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.locks.Lock;

@Slf4j
public final class TemporalRingBuffer {
    private final Long[] timeArr;
    private final int[] countArr;
    private final Lock sharedLock;
    private final int numOfSeconds;
    private final List<Long> seenTimeStamps;

    public TemporalRingBuffer(int bufferSizeInSeconds, final Lock sharedLock) {
        this.timeArr = new Long[bufferSizeInSeconds];
        this.countArr = new int[bufferSizeInSeconds];
        this.sharedLock = sharedLock;
        this.numOfSeconds = bufferSizeInSeconds - 1;
        seenTimeStamps = new ArrayList<>();
    }

    public void recordEvent(final LocalDateTime now) {
        long currentTimeEpochSecs = now.toEpochSecond(OffsetDateTime.now().getOffset());
        int index = (int) (currentTimeEpochSecs % numOfSeconds);
        sharedLock.lock();
        //log.info("index calculated : {} for epoch second : {} of current time", index, currentTimeEpochSecs);
        seenTimeStamps.add(currentTimeEpochSecs);
        if (Objects.isNull(timeArr[index])) {
            log.info("recording event at this index for the first time!");
            initEventCount(currentTimeEpochSecs, index);
        } else if (timeArr[index] != currentTimeEpochSecs) {
            log.info("existing timestamp : {} found at index : {} which is not the same as current timestamp : {}", timeArr[index], index, currentTimeEpochSecs);
            initEventCount(currentTimeEpochSecs, index);
        } else {
            log.debug("timestamp : {} at index : {} in timeArr same as current time, updating count", timeArr[index], index);
            countArr[index] += 1;
        }
        sharedLock.unlock();
    }

    private void initEventCount(long toEpochSecond, int index) {
        countArr[index] = 1;
        timeArr[index] = toEpochSecond;
    }

    public int getTotalEventsSeenInBuffer() {
        return Arrays.stream(countArr).sum();
    }

    public void printStats() {
        final Map<Long, Integer> map = new LinkedHashMap<>();
        this.seenTimeStamps.forEach(ts -> {
            map.putIfAbsent(ts, 1);
            map.computeIfPresent(ts, (k, v) -> v + 1);
        });
        map.forEach((k, v) -> log.info("key: {}, seenCount: {}", k, v));
        log.info("total events seen : {}", seenTimeStamps.size());
    }
}
