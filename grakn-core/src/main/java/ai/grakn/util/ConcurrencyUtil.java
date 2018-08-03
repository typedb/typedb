/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.util;

import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.Collection;
import java.util.List;

/**
 * <p>
 *     Concurrency convenience methogs
 * </p>
 *
 * @author Domenico Corapi
 */
public class ConcurrencyUtil {

    static public <T> Observable<List<T>> allObservable(Collection<Observable<T>> tasks) {
        return Observable.from(tasks)
                //execute in parallel
                .flatMap(task -> task.observeOn(Schedulers.computation()))
                //wait, until all task are executed
                //be aware, all your observable should emit onComplemete event
                //otherwise you will wait forever
                .toList();
    }

}
