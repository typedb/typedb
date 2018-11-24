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

package grakn.core.graql.query.pattern;

import grakn.core.graql.query.Graql;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class PatternTest {

    private final VarPattern x = Graql.var("x");
    private final VarPattern y = Graql.var("y");
    private final VarPattern z = Graql.var("z");
    private final VarPattern a = Graql.var("a");
    private final VarPattern b = Graql.var("b");
    private final VarPattern c = Graql.var("c");

    @Test
    public void testVarDNF() {
        assertHasDNF(set(conjunction(x)), x);
    }

    @Test
    public void testEmptyConjunctionDNF() {
        assertHasDNF(set(conjunction()), conjunction());
    }

    @Test
    public void testSingletonConjunctionDNF() {
        assertHasDNF(set(conjunction(x)), conjunction(x));
    }

    @Test
    public void testMultipleConjunctionDNF() {
        assertHasDNF(set(conjunction(x, y, z)), conjunction(x, y, z));
    }

    @Test
    public void testNestedConjunctionDNF() {
        assertHasDNF(set(conjunction(x, y, z)), conjunction(conjunction(x, y), z));
    }

    @Test
    public void testEmptyDisjunctionDNF() {
        assertHasDNF(set(), disjunction());
    }

    @Test
    public void testSingletonDisjunctionDNF() {
        assertHasDNF(set(conjunction(x)), disjunction(x));
    }

    @Test
    public void testMultipleDisjunctionDNF() {
        assertHasDNF(set(conjunction(x), conjunction(y), conjunction(z)), disjunction(x, y, z));
    }

    @Test
    public void testNestedDisjunctionDNF() {
        assertHasDNF(set(conjunction(x), conjunction(y), conjunction(z)), disjunction(disjunction(x, y), z));
    }

    @Test
    public void testDNFIdentity() {
        Set disjunction = set(conjunction(x, y, z), conjunction(a, b, c));
        assertHasDNF(disjunction, Graql.or(disjunction));
    }

    @Test
    public void testCNFToDNF() {
        Conjunction cnf = conjunction(disjunction(x, y, z), disjunction(a, b, c));
        Set<Conjunction<VarPattern>> dnf = set(
                conjunction(x, a), conjunction(x, b), conjunction(x, c),
                conjunction(y, a), conjunction(y, b), conjunction(y, c),
                conjunction(z, a), conjunction(z, b), conjunction(z, c)
        );

        assertHasDNF(dnf, cnf);
    }

    private <T extends Pattern> Conjunction<T> conjunction(T... patterns) {
        return Graql.and(Sets.newHashSet(patterns));
    }

    private <T extends Pattern> Disjunction<T> disjunction(T... patterns) {
        return Graql.or(Sets.newHashSet(patterns));
    }

    private <T extends Pattern> Set<T> set(T... patterns) {
        return Sets.newHashSet(patterns);
    }

    private void assertHasDNF(Set<Conjunction<VarPattern>> expected, Pattern pattern) {
        HashSet<Conjunction<VarPattern>> dnf = new HashSet<>(pattern.getDisjunctiveNormalForm().getPatterns());
        assertEquals(expected, dnf);
    }
}