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

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.pattern.Conjunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.vaticle.typedb.core.concept.answer.ConceptMap.Explainable.NOT_IDENTIFIED;

public class ExplainablesManager {

    private AtomicLong nextId;
    private ConcurrentMap<Long, Conjunction> conjunctions;
    private ConcurrentMap<Long, ConceptMap> bounds;

    public ExplainablesManager() {
        this.nextId = new AtomicLong(NOT_IDENTIFIED + 1);
        this.conjunctions = new ConcurrentHashMap<>();
        this.bounds = new ConcurrentHashMap<>();
    }

    public void setAndRecordExplainables(ConceptMap explainableMap) {
        explainableMap.explainables().iterator().forEachRemaining(explainable -> {
            long nextId = this.nextId.getAndIncrement();
            explainable.setId(nextId);
            conjunctions.put(nextId, explainable.conjunction());
            bounds.put(nextId, explainableMap);
        });
    }

    public Conjunction getConjunction(long explainableId) {
        return conjunctions.get(explainableId);
    }

    public ConceptMap getBounds(long explainableId) {
        return bounds.get(explainableId);
    }
}
