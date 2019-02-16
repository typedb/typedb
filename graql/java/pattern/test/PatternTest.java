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

package graql.lang.pattern.test;

import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Disjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static graql.lang.util.Collections.set;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class PatternTest {

    private final Statement x = Graql.var("x");
    private final Statement y = Graql.var("y");
    private final Statement z = Graql.var("z");
    private final Statement a = Graql.var("a");
    private final Statement b = Graql.var("b");
    private final Statement c = Graql.var("c");

    @Test
    public void testVarDNF() {
        assertHasDNF(set((Conjunction<Statement>[]) new Conjunction[]{conjunction(x)}), x);
    }

    @Test
    public void testEmptyConjunctionDNF() {
        assertHasDNF(set((Conjunction<Statement>[]) new Conjunction[]{conjunction()}), conjunction());
    }

    @Test
    public void testSingletonConjunctionDNF() {
        assertHasDNF(set((Conjunction<Statement>[]) new Conjunction[]{conjunction(x)}), conjunction(x));
    }

    @Test
    public void testMultipleConjunctionDNF() {
        assertHasDNF(set((Conjunction<Statement>[]) new Conjunction[]{conjunction(x, y, z)}), conjunction(x, y, z));
    }

    @Test
    public void testNestedConjunctionDNF() {
        assertHasDNF(set((Conjunction<Statement>[]) new Conjunction[]{conjunction(x, y, z)}), conjunction(conjunction(x, y), z));
    }

    @Test
    public void testEmptyDisjunctionDNF() {
        assertHasDNF(set((Conjunction<Statement>[]) new Conjunction[]{}), disjunction());
    }

    @Test
    public void testSingletonDisjunctionDNF() {
        assertHasDNF(set((Conjunction<Statement>[]) new Conjunction[]{conjunction(x)}), disjunction(x));
    }

    @Test
    public void testMultipleDisjunctionDNF() {
        assertHasDNF(set((Conjunction<Statement>[]) new Conjunction[]{conjunction(x), conjunction(y), conjunction(z)}), disjunction(x, y, z));
    }

    @Test
    public void testNestedDisjunctionDNF() {
        assertHasDNF(set((Conjunction<Statement>[]) new Conjunction[]{conjunction(x), conjunction(y), conjunction(z)}), disjunction(disjunction(x, y), z));
    }

    @Test
    public void testDNFIdentity() {
        Set disjunction = set((Conjunction<Statement>[]) new Conjunction[]{conjunction(x, y, z), conjunction(a, b, c)});
        assertHasDNF(disjunction, Graql.or(disjunction));
    }

    @Test
    public void testCNFToDNF() {
        Conjunction cnf = conjunction(disjunction(x, y, z), disjunction(a, b, c));
        Set<Conjunction<Statement>> dnf = set((Conjunction<Statement>[]) new Conjunction[]{conjunction(x, a), conjunction(x, b), conjunction(x, c), conjunction(y, a), conjunction(y, b), conjunction(y, c), conjunction(z, a), conjunction(z, b), conjunction(z, c)});

        assertHasDNF(dnf, cnf);
    }

    private <T extends Pattern> Conjunction<T> conjunction(T... patterns) {
        return Graql.and(set(patterns));
    }

    private <T extends Pattern> Disjunction<T> disjunction(T... patterns) {
        return Graql.or(set(patterns));
    }

    private void assertHasDNF(Set<Conjunction<Statement>> expected, Pattern pattern) {
        HashSet<Conjunction<Statement>> dnf = new HashSet<>(pattern.getDisjunctiveNormalForm().getPatterns());
        assertEquals(expected, dnf);
    }
}