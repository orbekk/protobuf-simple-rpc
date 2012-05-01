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

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import com.google.protobuf.Service;

public class ServiceHolder {
    private static final Logger logger = Logger.getLogger(
            ServiceHolder.class.getName());
    private final ConcurrentHashMap<String, Service> services =
            new ConcurrentHashMap<String, Service>();
    
    public ServiceHolder() {
    }
    
    public void registerService(Service service) {
        String serviceName = service.getDescriptorForType().getFullName();
        Service previousService = services.put(serviceName, service);
        if (previousService != null) {
            logger.warning("Replaced service " + previousService + " with " +
                    service);
        }
    }
    
    public void removeService(Service service) {
        String serviceName = service.getDescriptorForType().getFullName();
        removeService(serviceName);
    }
    
    public void removeService(String serviceName) {
        services.remove(serviceName);
    }
    
    public Service get(String fullServiceName) {
        return services.get(fullServiceName);
    }
}
