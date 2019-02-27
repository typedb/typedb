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

package grakn.core.graql.reasoner.atomic;

import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.Atomic;
import grakn.core.graql.reasoner.query.ReasonerQueries;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.statement.Statement;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class AtomicEquivalenceIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl genericSchemaSession;

    private static void loadFromFile(String fileName, SessionImpl session){
        try {
            InputStream inputStream = AtomicEquivalenceIT.class.getClassLoader().getResourceAsStream("test-integration/graql/reasoner/resources/"+fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            TransactionOLTP tx = session.transaction().write();
            Graql.parseList(s).forEach(tx::execute);
            tx.commit();
        } catch (Exception e){
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    private TransactionOLTP tx;

    @BeforeClass
    public static void loadContext(){
        genericSchemaSession = server.sessionWithNewKeyspace();
        loadFromFile("genericSchema.gql", genericSchemaSession);
    }

    @AfterClass
    public static void closeSession(){
        genericSchemaSession.close();
    }

    @Before
    public void setUp(){
        tx = genericSchemaSession.transaction().write();
    }

    @After
    public void tearDown(){
        tx.close();
    }

    @Test
    public void testEquality_DifferentIsaVariants(){
        testEquality_DifferentTypeVariants(tx, "isa", "baseRoleEntity", "subRoleEntity");
    }

    @Test
    public void testEquality_DifferentSubVariants(){
        testEquality_DifferentTypeVariants(tx, "sub", "baseRoleEntity", "baseRole1");
    }

    @Test
    public void testEquality_DifferentPlaysVariants(){
        testEquality_DifferentTypeVariants(tx, "plays", "baseRole1", "baseRole2");
    }

    @Test
    public void testEquality_DifferentRelatesVariants(){
        testEquality_DifferentTypeVariants(tx, "relates", "baseRole1", "baseRole2");
    }

    @Test
    public void testEquality_DifferentHasVariants(){
        String patternString = "{ $x has resource; };";
        String patternString2 = "{ $y has resource; };";
        String patternString3 = "{ $x has " + Graql.Token.Type.ATTRIBUTE + "; };";

        atomicEquality(patternString, patternString, true, tx);
        atomicEquality(patternString, patternString2, false, tx);
        atomicEquality(patternString, patternString3, false, tx);
        atomicEquality(patternString2, patternString3, false, tx);
    }

    @Test
    public void testEquivalence_DifferentRelationVariants(){
        String pattern = "{ (baseRole1: $x, baseRole2: $y) isa binary; };";
        String directPattern = "{ (baseRole1: $x, baseRole2: $y) isa! binary; };";
        String pattern2 = "{ $r (baseRole1: $x, baseRole2: $y) isa binary; };";
        String pattern3 = "{ $z (baseRole1: $x, baseRole2: $y) isa binary; };";
        String pattern4 = "{ (baseRole1: $x, baseRole2: $y); };";
        String pattern5 = "{ (baseRole1: $z, baseRole2: $v) isa binary; };";
        String pattern6 = "{ (role: $x, baseRole2: $y) isa binary; };";
        String pattern7 = "{ (baseRole1: $x, baseRole2: $y) isa $type; };";
        String pattern8 = "{ (baseRole1: $x, baseRole2: $y) isa $type;$type type binary; };";

        atomicEquality(pattern, pattern, true, tx);
        atomicEquality(pattern, directPattern, false, tx);
        atomicEquality(pattern, pattern2, false, tx);
        atomicEquality(pattern, pattern3, false, tx);
        atomicEquality(pattern, pattern4, false, tx);
        atomicEquality(pattern, pattern5, false, tx);
        atomicEquality(pattern, pattern6, false, tx);
        atomicEquality(pattern, pattern7, false, tx);
        atomicEquality(pattern, pattern8, false, tx);
    }

    private void testEquality_DifferentTypeVariants(TransactionOLTP tx, String keyword, String label, String label2){
        String variantAString = "{ $x " + keyword + " " + label + "; };";
        String variantAString2 = "{ $y " + keyword + " " + label + "; };";
        String variantAString3 = "{ $y " + keyword + " " + label2 + "; };";
        atomicEquality(variantAString, variantAString, true, tx);
        atomicEquality(variantAString, variantAString2, false, tx);
        atomicEquality(variantAString2, variantAString3, false, tx);

        String variantBString = "{ $x " + keyword + " $type; $type type " + label +"; };";
        String variantBString2 = "{ $x " + keyword + " $type; $type type " + label2 +"; };";
        String variantBString3 = "{ $x " + keyword + " $var; $var type " + label +"; };";
        String variantBString4 = "{ $y " + keyword + " $type; $type type " + label +"; };";
        atomicEquality(variantBString, variantBString, true, tx);
        atomicEquality(variantBString, variantBString2, false, tx);
        atomicEquality(variantBString, variantBString3, true, tx);
        atomicEquality(variantBString, variantBString4, false, tx);

        String variantCString = "{ $x " + keyword + " $y; };";
        String variantCString2 = "{ $x " + keyword + " $z; };";
        atomicEquality(variantCString, variantCString, true, tx);
        atomicEquality(variantCString, variantCString2, true, tx);

        atomicEquality(variantAString, variantBString, true, tx);
        atomicEquality(variantAString, variantCString, false, tx);
        atomicEquality(variantBString, variantCString, false, tx);
    }

    private void atomicEquality(String patternA, String patternB, boolean expectation, TransactionOLTP tx){
        Atom atomA = ReasonerQueries.atomic(conjunction(patternA, tx), tx).getAtom();
        Atom atomB = ReasonerQueries.atomic(conjunction(patternB, tx), tx).getAtom();
        atomicEquality(atomA, atomA, true);
        atomicEquality(atomB, atomB, true);
        atomicEquality(atomA, atomB, expectation);
        atomicEquality(atomB, atomA, expectation);
    }

    private void atomicEquality(Atomic a, Atomic b, boolean expectation){
        assertEquals("Atomic: " + a.toString() + " =? " + b.toString(), a.equals(b), expectation);

        //check hash additionally if need to be equal
        if (expectation) {
            assertEquals(a.toString() + " hash=? " + b.toString(), a.hashCode() == b.hashCode(), true);
        }
    }

    private Conjunction<Statement> conjunction(String patternString, TransactionOLTP tx){
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }
}
