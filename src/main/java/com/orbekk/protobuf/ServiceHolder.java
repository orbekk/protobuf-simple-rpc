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
    
    public void registerService (Service service) {
        String serviceName = service.getDescriptorForType().getFullName();
        Service previousService = services.put(serviceName, service);
        if (previousService != null) {
            logger.warning("Replaced service " + previousService + " with " +
                    service);
        }
    }
    
    public Service get(String fullServiceName) {
        return services.get(fullServiceName);
    }
}
