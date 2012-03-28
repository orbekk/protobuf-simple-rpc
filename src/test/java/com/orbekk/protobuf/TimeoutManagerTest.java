package com.orbekk.protobuf;

import static org.mockito.Mockito.*;

import java.io.Closeable;

import org.junit.Before;
import org.junit.Test;

import com.orbekk.protobuf.TimeoutManager.Environment;

public class TimeoutManagerTest {
    Closeable closeable = mock(Closeable.class);
    Environment environment = mock(Environment.class);
    TimeoutManager timeoutManager = new TimeoutManager(environment);
    
    @Before public void setUp() {
    }
    
    @Test public void closesExpiredEntries() throws Exception {
        when(environment.currentTimeMillis()).thenReturn(1000l);
        timeoutManager.addEntry(1000l, closeable);
        timeoutManager.performWork();
        verify(closeable).close();
    }
    
    @Test public void doesNotCloseUnexpiredEntry() throws Exception {
        when(environment.currentTimeMillis()).thenReturn(123456l);
        timeoutManager.addEntry(123457l, closeable);
        timeoutManager.performWork();
        verify(closeable, never()).close();
    }
}
