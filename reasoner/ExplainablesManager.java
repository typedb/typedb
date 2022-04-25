/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.reasoner;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Concludable;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.concept.answer.ConceptMap.Explainable.NOT_IDENTIFIED;

public class ExplainablesManager {

    private final AtomicLong nextId;
    private final ConcurrentMap<Long, Concludable> concludables;
    private final ConcurrentMap<Long, ConceptMap> bounds;

    public ExplainablesManager() {
        this.nextId = new AtomicLong(NOT_IDENTIFIED + 1);
        this.concludables = new ConcurrentHashMap<>();
        this.bounds = new ConcurrentHashMap<>();
    }

    public void setAndRecordExplainables(ConceptMap explainableMap) {
        explainableMap.explainables().iterator().forEachRemaining(explainable -> {
            long nextId = this.nextId.getAndIncrement();
            FunctionalIterator<Concludable> concludable = iterate(Concludable.create(explainable.conjunction()));
            assert concludable.hasNext();
            concludables.put(nextId, concludable.next());
            assert !concludable.hasNext();
            bounds.put(nextId, explainableMap);
            explainable.setId(nextId);
        });
    }

    public Concludable getConcludable(long explainableId) {
        return concludables.get(explainableId);
    }

    public ConceptMap getBounds(long explainableId) {
        return bounds.get(explainableId);
    }
}
