/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.util;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import static java.util.stream.Collectors.toList;
import rx.Observable;
import rx.schedulers.Schedulers;

/**
 * <p>
 *     Concurrency convenience methogs
 * </p>
 *
 * @author Domenico Corapi
 */
public class ConcurrencyUtil {
    static public <T> CompletableFuture<List<T>> all(Collection<CompletableFuture<T>> cf) {
        return CompletableFuture.allOf(cf.toArray(new CompletableFuture[cf.size()]))
                .thenApply(v -> cf.stream()
                        .map(CompletableFuture::join)
                        .collect(toList())
                );
    }

    static public <T> Observable<List<T>> allObservable(Collection<Observable<T>> tasks) {
        return Observable.from(tasks)
                //execute in parallel
                .flatMap(task -> task.observeOn(Schedulers.computation()))
                //wait, until all task are executed
                //be aware, all your observable should emit onComplemete event
                //otherwise you will wait forever
                .toList();
    }

    static public <T> Observable<List<T>> allObservableWithTimeout(Collection<Observable<T>> tasks, int time, TimeUnit timeUnit) {
        return Observable.from(tasks)
                .timeout(time, timeUnit)
                .flatMap(task -> task.observeOn(Schedulers.computation()))
                .toList();
    }
}
