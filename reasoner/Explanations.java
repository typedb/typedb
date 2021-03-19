/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.reasoner;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.pattern.Conjunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static grakn.core.concept.answer.ConceptMap.Explainable.NOT_IDENTIFIED;

public class Explanations {

    private AtomicLong explainableId;
    private ConcurrentMap<Long, ConceptMap.Explainable> explainableRegistry;

    public Explanations() {
        this.explainableId = new AtomicLong(NOT_IDENTIFIED + 1);
        this.explainableRegistry = new ConcurrentHashMap<>();
    }

    public void setAndRecordExplainableIds(ConceptMap explainableAnswer) {
        explainableAnswer.explainables().forEach(explainable -> {
            long nextId = explainableId.getAndIncrement();
            explainable.setId(nextId);
            explainableRegistry.put(nextId, explainable);
        });
    }

    public Conjunction getExplainableConjunction(long explainableId) {
        return explainableRegistry.get(explainableId).conjunction();
    }
}
