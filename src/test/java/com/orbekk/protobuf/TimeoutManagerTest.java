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
