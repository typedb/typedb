/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.query;

import com.google.common.collect.Sets;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.query.*;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class PatternImplTest {

    private final VarAdmin x = new VarImpl("x").admin();
    private final VarAdmin y = new VarImpl("y").admin();
    private final VarAdmin z = new VarImpl("z").admin();
    private final VarAdmin a = new VarImpl("a").admin();
    private final VarAdmin b = new VarImpl("b").admin();
    private final VarAdmin c = new VarImpl("c").admin();

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
        assertHasDNF(disjunction, new DisjunctionImpl<>(disjunction));
    }

    @Test
    public void testCNFToDNF() {
        Conjunction cnf = conjunction(disjunction(x, y, z), disjunction(a, b, c));
        Set<Conjunction<VarAdmin>> dnf = set(
                conjunction(x, a), conjunction(x, b), conjunction(x, c),
                conjunction(y, a), conjunction(y, b), conjunction(y, c),
                conjunction(z, a), conjunction(z, b), conjunction(z, c)
        );

        assertHasDNF(dnf, cnf);
    }

    private <T extends PatternAdmin> Conjunction<T> conjunction(T... patterns) {
        return new ConjunctionImpl<>(Sets.newHashSet(patterns));
    }

    private <T extends PatternAdmin> Disjunction<T> disjunction(T... patterns) {
        return new DisjunctionImpl<>(Sets.newHashSet(patterns));
    }

    private <T extends PatternAdmin> Set<T> set(T... patterns) {
        return Sets.newHashSet(patterns);
    }

    private void assertHasDNF(Set<Conjunction<VarAdmin>> expected, PatternAdmin pattern) {
        HashSet<Conjunction<VarAdmin>> dnf = new HashSet<>(pattern.getDisjunctiveNormalForm().getPatterns());
        assertEquals(expected, dnf);
    }
}