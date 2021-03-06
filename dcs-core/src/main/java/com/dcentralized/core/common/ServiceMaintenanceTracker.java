/*
 * Copyright (c) 2014-2015 dCentralizedSystems, LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.dcentralized.core.common;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.dcentralized.core.common.Service.ProcessingStage;
import com.dcentralized.core.common.Service.ServiceOption;

/**
 * Sequences service periodic maintenance
 */
class ServiceMaintenanceTracker {
    /**
     * Simple epsilon that is subtracted from absolute future expiration time. Maintenance
     * task scheduling is prone to JVM and OS thread scheduling variance, in addition to
     * misbehaving service handlers.
     * in the future, if more accuracy is required, a predictive scheme can be used
     */
    public static final long SCHEDULING_EPSILON_MICROS =
            TimeUnit.MILLISECONDS.toMicros(10);

    public static ServiceMaintenanceTracker create(ServiceHost host) {
        ServiceMaintenanceTracker smt = new ServiceMaintenanceTracker();
        smt.host = host;
        return smt;
    }

    private ServiceHost host;

    private ConcurrentHashMap<String, Long> trackedServices = new ConcurrentHashMap<>();
    private ConcurrentSkipListMap<Long, Set<String>> nextExpiration = new ConcurrentSkipListMap<>();

    public void schedule(Service s, long now) {
        long interval = s.getMaintenanceIntervalMicros();
        if (interval == 0) {
            interval = this.host.getMaintenanceIntervalMicros();
        }

        if (interval < this.host.getMaintenanceCheckIntervalMicros()) {
            this.host.setMaintenanceCheckIntervalMicros(interval);
        }

        long nextExpirationMicros = Math.max(now, now + interval - SCHEDULING_EPSILON_MICROS);
        String selfLink = s.getSelfLink();

        synchronized (this) {
            // To avoid double scheduling the same self-link
            // we lookup the self-link in our trackedServices map and remove
            // it before adding the new schedule.
            Long expiration = this.trackedServices.get(selfLink);
            if (expiration != null) {
                Set<String> services = this.nextExpiration.get(expiration);
                if (services != null) {
                    services.remove(selfLink);
                }
            }

            this.trackedServices.put(selfLink, nextExpirationMicros);
            Set<String> services = this.nextExpiration.get(nextExpirationMicros);
            if (services == null) {
                services = new HashSet<>();
                this.nextExpiration.put(nextExpirationMicros, services);
            }
            services.add(selfLink);
        }
    }

    public void performMaintenance(Operation op, long deadline) {
        long now = Utils.getSystemNowMicrosUtc();
        // at least one set of expired service maintained regardless of deadline
        do {
            if (this.host.isStopping()) {
                op.fail(new CancellationException("Host is stopping"));
                return;
            }

            Entry<Long, Set<String>> e;

            // the nextExpiration map is a concurrent data structure, but since each value is a Set, we want
            // to make sure modifications to that Set are not lost if a concurrent add() is happening
            synchronized (this) {
                // get any services set to expire within the current maintenance interval
                e = this.nextExpiration.firstEntry();
                if (e == null || e.getKey() >= now) {
                    // no service requires maintenance, yet
                    return;
                }
                this.nextExpiration.pollFirstEntry();
            }

            Long expiration = e.getKey();
            Set<String> services = e.getValue();

            for (String servicePath : services) {
                Service s = this.host.findService(servicePath);

                boolean skipMaintenance = s == null ||
                        s.getProcessingStage() != ProcessingStage.AVAILABLE ||
                        !s.hasOption(ServiceOption.PERIODIC_MAINTENANCE);

                if (skipMaintenance) {
                    checkAndRemoveFromMaintenance(servicePath, expiration);
                    continue;
                }

                if (!s.hasOption(ServiceOption.OWNER_SELECTION)) {
                    performServiceMaintenance(servicePath, s);
                    continue;
                }

                // service has OWNER_SELECTION - we need to check ownership
                long ownershipCheckExpiration = Math.max(deadline,
                        Utils.fromNowMicrosUtc(this.host.getOperationTimeoutMicros()));
                Utils.checkAndUpdateDocumentOwnership(this.host, s, ownershipCheckExpiration,
                        (o, ex) -> {
                            if (ex != null) {
                                this.host.log(Level.WARNING,
                                        "Failed to determine ownership for service %s: %s - skipping maintenance this time",
                                        servicePath, ex);
                                schedule(s, Utils.getNowMicrosUtc());
                                return;
                            }

                            if (!s.hasOption(ServiceOption.DOCUMENT_OWNER)) {
                                schedule(s, Utils.getNowMicrosUtc());
                                return;
                            }

                            performServiceMaintenance(servicePath, s);
                        });
            }
        } while ((now = Utils.getSystemNowMicrosUtc()) < deadline);
    }

    private void checkAndRemoveFromMaintenance(String servicePath, Long expiration) {
        synchronized (this) {
            // Another request scheduling this service's maintenance could
            // have occurred. So double check the expiration time, if it
            // matches the current expiration window, then remove it.
            Long serviceExpiration = this.trackedServices.get(servicePath);
            if (serviceExpiration.equals(expiration)) {
                this.trackedServices.remove(servicePath);
            }
        }
    }

    private void performServiceMaintenance(String servicePath, Service s) {
        long limit = Math.max(this.host.getMaintenanceIntervalMicros(),
                s.getMaintenanceIntervalMicros());
        Operation servicePost = Operation
                .createPost(null)
                .setReferer(this.host.getUri())
                .setBodyNoCloning(ServiceMaintenanceRequest.PERIODIC_INSTANCE)
                .setCompletion(
                        (o, ex) -> {
                            long now = 0;
                            try {
                                now = Utils.getSystemNowMicrosUtc();
                                long actual = now - (o.getExpirationMicrosUtc() - limit);
                                if (s.hasOption(ServiceOption.INSTRUMENTATION)) {
                                    updateStats(s, actual, limit, servicePath);
                                }
                                if (ex != null) {
                                    throw ex;
                                }
                            } catch (Throwable exx) {
                                this.host.log(Level.WARNING, "Service %s failed maintenance: %s",
                                        servicePath, Utils.toString(exx));
                            } finally {
                                // schedule again, for next maintenance interval
                                schedule(s, now);
                            }
                        });

        Runnable t = () -> {
            try {
                OperationContext.setAuthorizationContext(this.host
                        .getSystemAuthorizationContext());
                if (s.hasOption(Service.ServiceOption.INSTRUMENTATION)) {
                    s.adjustStat(Service.STAT_NAME_MAINTENANCE_COUNT, 1);
                }
                servicePost.setExpiration(Utils.getSystemNowMicrosUtc() + limit);
                s.handleMaintenance(servicePost);
            } catch (Exception ex) {
                // Mostly at this point, CompletionHandler for servicePost has already consumed in
                // "s.handleMaintenance()" and have set null (based on the handleMaintenance impl).
                // Calling fail() will not trigger any CompletionHandler, therefore explicitly
                // write log here as well.
                this.host.log(Level.WARNING, "Service %s failed to perform maintenance: %s",
                        servicePath, Utils.toString(ex));
                servicePost.fail(ex);
            }
        };

        if (s.hasOption(ServiceOption.CORE)) {
            this.host.getCoreScheduledExecutor().execute(t);
        } else {
            this.host.getScheduledExecutor().execute(t);
        }
    }

    public synchronized void close() {
        this.trackedServices.clear();
        this.nextExpiration.clear();
    }

    private void updateStats(Service s, long actual, long limit, String servicePath) {
        ServiceStats.ServiceStat durationStat = ServiceStatUtils.getOrCreateHistogramStat(s,
                Service.STAT_NAME_MAINTENANCE_DURATION);
        s.setStat(durationStat, actual);
        if (limit * 10 < actual) {
            this.host.log(Level.WARNING,
                    "Service %s exceeded maintenance interval %d. Actual: %d",
                    servicePath, limit, actual);
            s.adjustStat(
                    Service.STAT_NAME_MAINTENANCE_COMPLETION_DELAYED_COUNT, 1);
        }
    }
}
