/**
 * Copyright 2012 Kjetil Ørbekk <kjetil.orbekk@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orbekk.protobuf;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.PriorityQueue;
import java.io.Closeable;
import java.io.IOException;

public class TimeoutManager extends Thread {
    private static final Logger logger =
            Logger.getLogger(TimeoutManager.class.getName());
    private Environment environment;
    private PriorityQueue<Entry> entries = new PriorityQueue<Entry>();

    private static class Entry implements Comparable<Entry> {
        // May not be null.
        public Long timeout;
        public Closeable closeable;

        public Entry(long timeout, Closeable closeable) {
            this.timeout = timeout;
            this.closeable = closeable;
        }

        public void close() throws IOException {
            closeable.close();
        }
        
        @Override public int compareTo(Entry other) {
            return timeout.compareTo(other.timeout);
        }
    }

    public interface Environment {
        long currentTimeMillis();
        void sleep(long millis) throws InterruptedException;
    }

    public static class DefaultEnvironment implements Environment {
        @Override public long currentTimeMillis() {
            return System.currentTimeMillis();
        }
        @Override public void sleep(long millis) throws InterruptedException {
            Thread.sleep(millis);
        }
    }

    TimeoutManager(Environment environment) {
        this.environment = environment;
    }

    public TimeoutManager() {
        this(new DefaultEnvironment());
    }

    public synchronized void performWork() {
        try {
            if (entries.isEmpty()) {
                wait();
            } else {
                long sleepTime = entries.peek().timeout -
                        environment.currentTimeMillis();
                if (sleepTime > 0) {
                    wait(sleepTime);
                }
            }
            closeExpiredEntries();
        } catch (InterruptedException e) {
            // Handled by outer loop.
        }
    }
    
    @Override public void run() {
        while (!Thread.interrupted()) {
            performWork();
        }
    }

    public synchronized void closeExpiredEntries() {
        long currentTime = environment.currentTimeMillis();
        while (!entries.isEmpty() && entries.peek().timeout <= currentTime) {
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
