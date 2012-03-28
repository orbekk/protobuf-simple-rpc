package com.orbekk.protobuf;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.PriorityQueue;
import java.io.Closeable;

public class TimeoutManager extends Thread {
    private static final Logger logger =
            Logger.getLogger(TimeoutManager.class.getName());
    private Environment environment;
    private PriorityQueue<Entry> entries = new PriorityQueue<Entry>();

    private static class Entry implements Comparable<? extends Entry> {
        // May not be null.
        public Long timeout;
        public Closeable closeable;

        public Entry(long timeout, Closeable closeable) {
            this.timeout = timeout;
            this.closeable = closeable;
        }

        @Override public int compareTo(Entry other) {
            return timeout.compareTo(other.timeout);
        }
    }

    public interface Environment {
        long currentTimeMillis();
        void sleep(long millis) throws InterruptedException;
    }

    public static class DefaultEnvironment {
        @Override public long currentTimeMillis() {
            System.currentTimeMillis();
        }
        @Override public void sleep(long millis) throws InterruptedException {
            Thread.sleep(millis);
        }
    }

    TimeoutManager(Environment environment) {
        this.environment = environment;
    }

    public TimeoutManager() {
        self(new DefaultTime());
    }

    @Override public void run() {
        while (!Thread.interrupted()) {
            synchronized (this) {
                if (entries.isEmpty()) {
                    environment.wait();
                } else {
                    long sleepTime = entries.peek().timeout -
                            environment.currentTimeMillis();
                    if (sleepTime > 0) {
                        environment.sleep(sleepTime);
                    }
                }
                closeExpiredEntries();
            }
        }
    }

    public synchronized void closeExpiredEntries() {
        long currentTime = environment.currentTimeMillis();
        while (entries.peek().timeout <= currentTime) {
            try {
                entries.poll().close();
            } catch (IOException e) {
                logger.log(Level.INFO, "Could not close entry. ", e);
            }
        }
    }

    public synchronized void addEntry(long timeoutTime, Closeable closeable) {
        if (entries.isEmpty() || timeoutTime <= entries.peek().timeout) {
            notify();
        }
        entries.add(new Entry(timeoutTime, closeable));
    }
}
