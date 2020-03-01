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
 *
 */

package grakn.core.graql.reasoner.query;

import grakn.core.graql.reasoner.atom.AtomicEquivalence;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.statement.Statement;

import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryTestUtil {

    public static MultiUnifier unification(ReasonerAtomicQuery child, ReasonerAtomicQuery parent, boolean unifierExists, UnifierType unifierType) {
        if (unifierType.equivalence() != null) {
            queryEquivalence(child, parent, unifierExists, unifierType.equivalence());
        }
        MultiUnifier multiUnifier = child.getMultiUnifier(parent, unifierType);

        assertEquals("Unexpected unifier: " + multiUnifier + " between the child - parent pair:\n" + child + " :\n" + parent, unifierExists, !multiUnifier.isEmpty());
        if (unifierExists && unifierType.equivalence() != null) {
            MultiUnifier multiUnifierInverse = parent.getMultiUnifier(child, unifierType);
            assertEquals("Unexpected unifier inverse: " + multiUnifier + " of type " + unifierType.name() + " between the child - parent pair:\n" + parent + " :\n" + child, unifierExists, !multiUnifierInverse.isEmpty());
            assertEquals(multiUnifierInverse, multiUnifier.inverse());
        }
        return multiUnifier;
    }

    public static void queryEquivalence(ReasonerAtomicQuery a, ReasonerAtomicQuery b, boolean queryExpectation, ReasonerQueryEquivalence equiv){
        singleQueryEquivalence(a, a, true, equiv);
        singleQueryEquivalence(b, b, true, equiv);
        singleQueryEquivalence(a, b, queryExpectation, equiv);
        singleQueryEquivalence(b, a, queryExpectation, equiv);
    }

    public static void atomicEquivalence(Atomic a, Atomic b, boolean expectation, AtomicEquivalence equiv){
        singleAtomicEquivalence(a, a, true, equiv);
        singleAtomicEquivalence(b, b, true, equiv);
        singleAtomicEquivalence(a, b, expectation, equiv);
        singleAtomicEquivalence(b, a, expectation, equiv);
    }

    private static void singleQueryEquivalence(ReasonerAtomicQuery a, ReasonerAtomicQuery b, boolean queryExpectation, ReasonerQueryEquivalence equiv){
        assertEquals(equiv.name() + " - Query:\n" + a + "\n=?\n" + b, queryExpectation, equiv.equivalent(a, b));

        //check hash additionally if need to be equal
        if (queryExpectation) {
            assertTrue(equiv.name() + ":\n" + a + "\nhash=?\n" + b, equiv.hash(a) == equiv.hash(b));
        }
    }

    private static void singleAtomicEquivalence(Atomic a, Atomic b, boolean expectation, AtomicEquivalence equivalence){
        assertEquals(equivalence.name() + " - Atom:\n" + a + "\n=?\n" + b, expectation,  equivalence.equivalent(a, b));

        //check hash additionally if need to be equal
        if (expectation) {
            assertTrue(equivalence.name() + ":\n" + a + "\nhash=?\n" + b, equivalence.hash(a) == equivalence.hash(b));
        }
    }

    public static Conjunction<Statement> conjunction(String patternString) {
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }
}
