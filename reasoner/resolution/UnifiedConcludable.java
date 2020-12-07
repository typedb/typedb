/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.reasoner.resolution;

import grakn.common.concurrent.actor.Actor;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.pattern.variable.Variable;
import grakn.core.reasoner.resolution.resolver.ConcludableResolver;

import java.util.Map;

public class UnifiedConcludable {
    private final Actor<ConcludableResolver> concludable;
    private final Map<Variable, Variable> variableMapping;

    UnifiedConcludable(Actor<ConcludableResolver> concludable, Map<Variable, Variable> variableMapping) {
        this.concludable = concludable;
        this.variableMapping = variableMapping;
    }

    public Actor<ConcludableResolver> concludable() {
        return concludable;
    }

    public Map<Variable, Variable> variableMapping() {
        return variableMapping;
    }

    public UnifiedConceptMap unify(ConceptMap conceptMap) {
        return new UnifiedConceptMap(conceptMap, variableMapping);
    }

}
