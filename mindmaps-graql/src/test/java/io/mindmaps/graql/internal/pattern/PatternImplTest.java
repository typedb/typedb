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

package io.mindmaps.graql.internal.pattern;

import com.google.common.collect.Sets;
import io.mindmaps.graql.api.query.Pattern;
import io.mindmaps.graql.api.query.Var;
import io.mindmaps.graql.internal.query.VarImpl;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class PatternImplTest {

    private final Var.Admin x = new VarImpl("x").admin();
    private final Var.Admin y = new VarImpl("y").admin();
    private final Var.Admin z = new VarImpl("z").admin();
    private final Var.Admin a = new VarImpl("a").admin();
    private final Var.Admin b = new VarImpl("b").admin();
    private final Var.Admin c = new VarImpl("c").admin();

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
        assertHasDNF(disjunction, Pattern.Admin.disjunction(disjunction));
    }

    @Test
    public void testCNFToDNF() {
        Pattern.Conjunction cnf = conjunction(disjunction(x, y, z), disjunction(a, b, c));
        Set<Pattern.Conjunction<Var.Admin>> dnf = set(
                conjunction(x, a), conjunction(x, b), conjunction(x, c),
                conjunction(y, a), conjunction(y, b), conjunction(y, c),
                conjunction(z, a), conjunction(z, b), conjunction(z, c)
        );

        assertHasDNF(dnf, cnf);
    }

    private <T extends Pattern.Admin> Pattern.Conjunction<T> conjunction(T... patterns) {
        return Pattern.Admin.conjunction(Sets.newHashSet(patterns));
    }

    private <T extends Pattern.Admin> Pattern.Disjunction<T> disjunction(T... patterns) {
        return Pattern.Admin.disjunction(Sets.newHashSet(patterns));
    }

    private <T extends Pattern.Admin> Set<T> set(T... patterns) {
        return Sets.newHashSet(patterns);
    }

    private void assertHasDNF(Set<Pattern.Conjunction<Var.Admin>> expected, Pattern.Admin pattern) {
        HashSet<Pattern.Conjunction<Var.Admin>> dnf = new HashSet<>(pattern.getDisjunctiveNormalForm().getPatterns());
        assertEquals(expected, dnf);
    }
}