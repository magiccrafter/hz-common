/*
 *
 *  * Copyright 2016 Nasko Vasilev
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.github.magiccrafter.hazelcast.common;

import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Utility class for Hazelcast maps operations where acquiring a lock is required.
 *
 * @author vasilevn
 */
public final class MapUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MapUtils.class);

    private MapUtils() {}

    public static <T,U,V> V returnWithLock(MultiMap<T, U> map, T key, V failResponse,
                                           BiFunction<MultiMap<T, U>, T, V> coreFunction) {
        return returnWithLock(map, map.getName(), key, failResponse,
                MultiMap::tryLock,
                coreFunction,
                MultiMap::unlock);
    }

    public static <T,U,V> V returnWithLock(IMap<T, U> map, T key, V failResponse,
                                           BiFunction<IMap<T, U>, T, V> coreFunction) {
        return returnWithLock(map, map.getName(), key, failResponse,
                IMap::tryLock,
                coreFunction,
                IMap::unlock);
    }

    public static <T,U,V> V returnWithLock(long time, TimeUnit timeUnit,
                                           MultiMap<T, U> map, T key, V failResponse,
                                           BiFunction<MultiMap<T, U>, T, V> coreFunction) {
        return returnWithLock(map, map.getName(), key, failResponse,
                (m, k) -> m.tryLock(k, time, timeUnit),
                coreFunction,
                MultiMap::unlock);
    }

    public static <T,U,V> V returnWithLock(long time, TimeUnit timeUnit,
                                           IMap<T, U> map, T key, V failResponse,
                                           BiFunction<IMap<T, U>, T, V> coreFunction) {
        return returnWithLock(map, map.getName(), key, failResponse,
                (m, k) -> m.tryLock(k, time, timeUnit),
                coreFunction,
                IMap::unlock);
    }

    private static <M, K, R> R returnWithLock(M map, String mapName, K key, R failResponse,
                                              TryLockPredicate<M, K> tryLockPredicate,
                                              BiFunction<M, K, R> coreFunction,
                                              BiConsumer<M, K> unlockFunction) {
        try {
            LOG.debug("Trying to acquire a lock on map={} for key={}", mapName, key);
            if (tryLockPredicate.test(map, key)) {
                return coreFunction.apply(map, key);
            }
        } catch (InterruptedException e) {
            LOG.error("Could not acquire a lock on map={} for key={}", mapName, key);
        } finally {
            LOG.debug("Releasing lock on map={} for key={}", mapName, key);
            unlockFunction.accept(map, key);
        }

        return failResponse;
    }

    @FunctionalInterface
    private interface TryLockPredicate<N, A> {
        boolean test(N n, A a) throws InterruptedException;
    }
}
