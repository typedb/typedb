/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.answer;

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
