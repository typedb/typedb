/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.reasoner.answer;

import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Explanation extends PartialExplanation {

    private final Map<Retrievable, Set<Variable>> variableMapping;
    private final int hash;

    public Explanation(Rule rule, Map<Retrievable, Set<Variable>> variableMapping,
                       ConclusionAnswer conclusionAnswer, ConceptMap conditionAnswer) {
        super(rule, conclusionAnswer, conditionAnswer);
        this.variableMapping = variableMapping;
        this.hash = Objects.hash(super.hashCode(), variableMapping);
    }

    public Map<Retrievable, Set<Variable>> variableMapping() {
        return variableMapping;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Explanation that = (Explanation) o;
        return variableMapping.equals(that.variableMapping);
    }

    @Override
    public int hashCode() {
        return this.hash;
    }
}
