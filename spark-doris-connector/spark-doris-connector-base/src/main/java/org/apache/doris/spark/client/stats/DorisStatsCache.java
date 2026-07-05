// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.client.stats;

import org.apache.doris.spark.client.entity.DorisColumnStats;
import org.apache.doris.spark.client.entity.DorisTableStats;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * JVM-level singleton cache for Doris statistics.
 *
 * <p>Two caches:
 * <ul>
 *     <li>table-level: key = {@code feEndpoints#db#table} -> {@code Optional<DorisTableStats>}</li>
 *     <li>column-level: key = {@code feEndpoints#db#table#sortedColsHash} -> {@code Map<String, DorisColumnStats>}</li>
 * </ul>
 *
 * <p>Caches are process-wide and shared across Spark sessions. TTL is configurable per-load
 * via {@link #reload(long)}; defaults to 5 minutes.
 */
public final class DorisStatsCache {

    private static final Logger LOG = LoggerFactory.getLogger(DorisStatsCache.class);

    private static volatile DorisStatsCache instance;

    private volatile Cache<String, Optional<DorisTableStats>> tableStatsCache;
    private volatile Cache<String, Map<String, DorisColumnStats>> columnStatsCache;

    private DorisStatsCache(long ttlMs) {
        reload(ttlMs);
    }

    public static DorisStatsCache getInstance(long ttlMs) {
        DorisStatsCache local = instance;
        if (local == null || local.currentTtl != ttlMs) {
            synchronized (DorisStatsCache.class) {
                local = instance;
                if (local == null || local.currentTtl != ttlMs) {
                    instance = new DorisStatsCache(ttlMs);
                    local = instance;
                }
            }
        }
        return local;
    }

    private volatile long currentTtl = -1L;

    public void reload(long ttlMs) {
        this.currentTtl = ttlMs;
        this.tableStatsCache = CacheBuilder.newBuilder()
                .expireAfterWrite(ttlMs, TimeUnit.MILLISECONDS)
                .maximumSize(1024)
                .build();
        this.columnStatsCache = CacheBuilder.newBuilder()
                .expireAfterWrite(ttlMs, TimeUnit.MILLISECONDS)
                .maximumSize(2048)
                .build();
        LOG.debug("DorisStatsCache reloaded with ttl={}ms", ttlMs);
    }

    public Optional<DorisTableStats> getTableStats(String key) {
        return tableStatsCache.getIfPresent(key);
    }

    public void putTableStats(String key, Optional<DorisTableStats> stats) {
        if (stats != null) {
            tableStatsCache.put(key, stats);
        }
    }

    public Map<String, DorisColumnStats> getColumnStats(String key) {
        return columnStatsCache.getIfPresent(key);
    }

    public void putColumnStats(String key, Map<String, DorisColumnStats> stats) {
        if (stats != null) {
            columnStatsCache.put(key, stats);
        }
    }

    public static String tableKey(String feEndpoints, String db, String table) {
        return feEndpoints + "#" + db + "#" + table;
    }

    public static String columnKey(String feEndpoints, String db, String table, int sortedColsHash) {
        return feEndpoints + "#" + db + "#" + table + "#" + sortedColsHash;
    }

    public void invalidateAll() {
        tableStatsCache.invalidateAll();
        columnStatsCache.invalidateAll();
    }
}
