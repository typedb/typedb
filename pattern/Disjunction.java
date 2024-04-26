/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.pattern;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.pattern.Conjunctable;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.CURLY_CLOSE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.CURLY_OPEN;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.NEW_LINE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Operator.OR;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class Disjunction implements Pattern, Cloneable {

    private final List<Conjunction> conjunctions;
    private final int hash;
    private final Set<Identifier.Variable.Name> sharedVariables; // set of common variables shared across branches of the disjunction
    private final Set<Identifier.Variable.Name> namedVariables; // set of named variables used in anywhere

    public Disjunction(List<Conjunction> conjunctions) {
        this.conjunctions = conjunctions;
        this.hash = Objects.hash(conjunctions);
        this.sharedVariables = iterate(conjunctions)
                .flatMap(conjunction -> iterate(conjunction.retrieves()))
                .filter(id -> id.isName() && iterate(conjunctions).allMatch(conjunction -> conjunction.retrieves().contains(id)))
                .map(Identifier.Variable::asName).toSet();
        this.namedVariables = iterate(conjunctions)
                .flatMap(conjunction ->
                        iterate(conjunction.retrieves()).filter(Identifier::isName).map(Identifier.Variable::asName)
                                .link(
                                        iterate(conjunction.negations()).flatMap(negation -> iterate(negation.disjunction().namedVariables()))
                                )
                ).toSet();
        // TODO: we should validate that named vars are not assigned to clashing thing/type/value classes (in TypeQL?)
    }

    public static Disjunction create(
            com.vaticle.typeql.lang.pattern.Disjunction<com.vaticle.typeql.lang.pattern.Conjunction<Conjunctable>> typeql) {
        return create(typeql, null);
    }

    public static Disjunction create(
            com.vaticle.typeql.lang.pattern.Disjunction<com.vaticle.typeql.lang.pattern.Conjunction<Conjunctable>> typeql,
            @Nullable VariableRegistry bounds) {
        return new Disjunction(typeql.patterns().stream().map(
                conjunction -> Conjunction.create(conjunction, bounds)
        ).collect(toList()));
    }

    public List<Conjunction> conjunctions() {
        return conjunctions;
    }

    public boolean isCoherent() {
        return iterate(conjunctions).allMatch(Conjunction::isCoherent);
    }

    public Set<Identifier.Variable.Name> returnedVariables() {
        return sharedVariables;
    }

    public Set<Identifier.Variable.Name> namedVariables() {
        return namedVariables;
    }

    public FunctionalIterator<Label> getTypes(Identifier.Variable.Name id) {
        return iterate(conjunctions).flatMap(conjunction -> iterate(conjunction.variable(id).inferredTypes()));
    }

    @Override
    public Disjunction clone() {
        return new Disjunction(iterate(conjunctions).map(Conjunction::clone).toList());
    }

    @Override
    public String toString() {
        return conjunctions.stream().map(Conjunction::toString)
                .collect(joining("" + CURLY_CLOSE + NEW_LINE + OR + NEW_LINE + CURLY_OPEN,
                        "" + CURLY_OPEN, "" + CURLY_CLOSE));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Disjunction that = (Disjunction) o;
        // TODO: should use a set-comparison
        // TODO: corrected with https://github.com/vaticle/typedb/issues/6115
        return this.conjunctions.equals(that.conjunctions);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
