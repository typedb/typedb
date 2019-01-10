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

package grakn.core.graql.internal.reasoner.atom;

import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.internal.executor.property.PropertyExecutor;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Factory class for creating {@link Atomic} objects.
 */
public class AtomicFactory {



    
    /**
     * @param pattern conjunction of patterns to be converted to atoms
     * @param parent query the created atoms should belong to
     * @return set of atoms
     */
    public static Stream<Atomic> createAtoms(Conjunction<Statement> pattern, ReasonerQuery parent) {
        Set<Atomic> atoms = pattern.statements().stream()
                .flatMap(statement -> statement.properties().stream()
                        .map(property -> createAtom(property, statement, pattern.statements(), parent))
                        .filter(Objects::nonNull))
                .collect(Collectors.toSet());

        return atoms.stream()
                .filter(at -> atoms.stream()
                        .filter(Atom.class::isInstance)
                        .map(Atom.class::cast)
                        .flatMap(Atom::getInnerPredicates)
                        .noneMatch(at::equals)
                );
    }

    /**
     * maps a provided var property to a reasoner atom
     *
     * @param property {@link VarProperty} to map
     * @param statement    {@link Statement} this property belongs to
     * @param statements   Vars constituting the pattern this property belongs to
     * @param parent reasoner query this atom should belong to
     * @return created atom
     */
    private static Atomic createAtom(VarProperty property, Statement statement, Set<Statement> statements, ReasonerQuery parent){
        Atomic atomic = PropertyExecutor.create(statement.var(), property)
                .atomic(parent, statement, statements);
        if (atomic == null) return null;
        return statement.isPositive() ? atomic : NegatedAtomic.create(atomic);
    }

}

