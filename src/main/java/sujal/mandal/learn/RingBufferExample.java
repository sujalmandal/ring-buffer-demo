package sujal.mandal.learn;

import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

@Slf4j
public class RingBufferExample {

    public static void main(String[] args) throws InterruptedException {
        new RingBufferExample().run();
    }

    public void run() throws InterruptedException {
        int totalEvents = 5000;
        int totalTasks = 10;
        int threadPoolSize = 3;
        int totalTimeToKeepTrackOf = 3;
        final CountDownLatch latch = new CountDownLatch(totalEvents);
        long startTime = System.currentTimeMillis();
        final Lock sharedLock = new ReentrantLock();
        final TemporalRingBuffer temporalRingBuffer = new TemporalRingBuffer(totalTimeToKeepTrackOf, sharedLock);
        final ExecutorService service = Executors.newFixedThreadPool(threadPoolSize);

        IntStream.range(0, totalTasks).forEach(i -> service.execute(
                new Task(UUID.randomUUID().toString(), startTime, latch, temporalRingBuffer)));

        log.info("awaiting countdown..");
        latch.await();
        log.info("latch await finished()");
        service.shutdownNow();
        service.close();
        log.info("total events : {}", temporalRingBuffer.getTotalEventsSeenInBuffer());
        temporalRingBuffer.printStats();
    }

    public record Task(String taskName, long startTime, CountDownLatch latch,
                       TemporalRingBuffer temporalRingBuffer) implements Runnable {
        static final Random r = new Random();

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                boolean isInterruptedWhileSleep = simulateNetworkDelay();
                if (isInterruptedWhileSleep) return;
                //log.info("latch count : {}, time since start : {} ms", latch.getCount(), (System.currentTimeMillis() - startTime));
                temporalRingBuffer.recordEvent(LocalDateTime.now());
                //log.info("event recorded");
                latch.countDown();
            }
        }

        public boolean simulateNetworkDelay() {
            try {
                Thread.sleep(getRandom(10));
            } catch (InterruptedException e) {
                log.warn("interrupted during sleep!");
                return true;
            }
            return false;
        }

        public int getRandom(int upperBound) {
            return r.nextInt(upperBound);
        }
    }

}