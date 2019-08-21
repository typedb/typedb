/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import grakn.core.concept.type.AttributeType;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.Atomic;
import grakn.core.graql.reasoner.atom.AtomicEquivalence;
import grakn.core.graql.reasoner.query.ReasonerQueries;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.kb.concept.ValueConverter;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class AtomicEquivalenceIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl genericSchemaSession;

    private TransactionOLTP tx;

    @BeforeClass
    public static void loadContext(){
        genericSchemaSession = server.sessionWithNewKeyspace();
        String resourcePath = "test-integration/graql/reasoner/resources/";
        loadFromFileAndCommit(resourcePath,"genericSchema.gql", genericSchemaSession);
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
    public void testEquality_DifferentRelationVariants(){
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

    private static Map<AttributeType.DataType<?>, Object> testValues = ImmutableMap.<AttributeType.DataType<?>, Object>builder()
            .put(AttributeType.DataType.BOOLEAN, true)
            .put(AttributeType.DataType.DATE, LocalDateTime.now())
            .put(AttributeType.DataType.DOUBLE, 10.0)
            .put(AttributeType.DataType.FLOAT, 10.0)
            .put(AttributeType.DataType.INTEGER, 10)
            .put(AttributeType.DataType.LONG, 10L)
            .put(AttributeType.DataType.STRING, "10")
            .build();

    @Test
    public void testEquivalence_AttributesWithEquivalentValues(){
        AttributeType<?> metaAttributeType = tx.getMetaAttributeType();
        Set<AttributeType> attributeTypes = metaAttributeType.subs().collect(toSet());

        AttributeType.DataType.values().stream()
                .filter(dataType -> attributeTypes.stream().anyMatch(t -> t.dataType() != null && t.dataType().equals(dataType)))
                .forEach(dataType -> {
                    Object value = testValues.get(dataType);
                    attributeTypes.stream()
                            .filter(t -> Objects.nonNull(t.dataType()))
                            .filter(t -> t.dataType().equals(dataType)).forEach(attributeType -> {

                        Pattern basePattern = Graql.parsePattern("$x has " + attributeType.label().getValue() + " " + value + ";");
                        dataType.comparableDataTypes().forEach(comparableDataType -> {
                            ValueConverter<Object, ?> converter = ValueConverter.of(comparableDataType);
                            Pattern convertedPattern = Graql.parsePattern("$x has " + attributeType.label().getValue() + " " + converter.convert(value) + ";");
                            atomicEquivalence(basePattern.toString(), convertedPattern.toString(), true, AtomicEquivalence.AlphaEquivalence, tx);
                            atomicEquivalence(basePattern.toString(), convertedPattern.toString(), true, AtomicEquivalence.StructuralEquivalence, tx);
                        });
            });
        });
    }

    @Test
    public void testEquivalence_DifferentNonVariablePredicateVariants(){
        Statement value = Graql.var("value");
        final String bound = "value";
        ArrayList<Statement> variablePredicates = Lists.newArrayList(
                value.gt(bound),
                value.gte(bound),
                value.lt(bound),
                value.lte(bound),
                value.eq(bound),
                value.neq(bound),
                value.contains(bound)
        );

        variablePredicates.forEach(vp -> {
            Conjunction<Statement> conj = (Conjunction<Statement>) Graql.and(vp);
            Atomic atom = ReasonerQueries.create(conj, tx).getAtoms().stream().findFirst().orElse(null);
            Atomic atomCopy = atom.copy(atom.getParentQuery());

            atomicEquivalence(atom, atomCopy,true, AtomicEquivalence.Equality);
            atomicEquivalence(atom, atomCopy,true, AtomicEquivalence.AlphaEquivalence);
            atomicEquivalence(atom, atomCopy,true, AtomicEquivalence.StructuralEquivalence);
            variablePredicates.stream()
                    .filter(vp2 -> !vp.equals(vp2))
                    .forEach(vp2 -> {
                        Conjunction<Statement> conj2 = (Conjunction<Statement>) Graql.and(vp2);
                        Atomic atom2 = ReasonerQueries.create(conj2, tx).getAtoms().stream().findFirst().orElse(null);

                        atomicEquivalence(atom, atom2,false, AtomicEquivalence.Equality);
                        atomicEquivalence(atom, atom2,false, AtomicEquivalence.AlphaEquivalence);
                        atomicEquivalence(atom, atom2,false, AtomicEquivalence.StructuralEquivalence);
                    });
        });
    }

    @Test
    public void testEquivalence_DifferentVariablePredicateVariants(){
        Statement value = Graql.var("value");
        Statement anotherValue = Graql.var("anotherValue");
        ArrayList<Statement> variablePredicates = Lists.newArrayList(
                value.gt(anotherValue),
                value.gte(anotherValue),
                value.lt(anotherValue),
                value.lte(anotherValue),
                value.eq(anotherValue),
                value.neq(anotherValue),
                value.contains(anotherValue)
        );

        variablePredicates.forEach(vp -> {
            Conjunction<Statement> conj = (Conjunction<Statement>) Graql.and(vp);
            Atomic atom = ReasonerQueries.create(conj, tx).getAtoms().stream().findFirst().orElse(null);
            Atomic atomCopy = atom.copy(atom.getParentQuery());
            assertEquals(atom, atomCopy);
            assertTrue(atom.isAlphaEquivalent(atomCopy));
            assertTrue(atom.isStructurallyEquivalent(atomCopy));
            variablePredicates.stream()
                    .filter(vp2 -> !vp.equals(vp2))
                    .forEach(vp2 -> {
                        Conjunction<Statement> conj2 = (Conjunction<Statement>) Graql.and(vp2);
                        Atomic atom2 = ReasonerQueries.create(conj2, tx).getAtoms().stream().findFirst().orElse(null);
                        assertNotEquals("Unexpected equality outcome: " + atom + "==" + atom2, atom, atom2);
                        assertFalse("Unexpected alpha-equivalence outcome: " + atom + "==" + atom2, atom.isAlphaEquivalent(atom2));
                        assertFalse("Unexpected struct-equivalence outcome: " + atom + "==" + atom2, atom.isStructurallyEquivalent(atom2));
                    });
        });
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

    private void atomicEquivalence(String patternA, String patternB, boolean expectation, AtomicEquivalence equiv, TransactionOLTP tx){
        Atom atomA = ReasonerQueries.atomic(conjunction(patternA), tx).getAtom();
        Atom atomB = ReasonerQueries.atomic(conjunction(patternB), tx).getAtom();
        atomicEquivalence(atomA, atomA, true, equiv);
        atomicEquivalence(atomB, atomB, true, equiv);
        atomicEquivalence(atomA, atomB, expectation, equiv);
        atomicEquivalence(atomB, atomA, expectation, equiv);
    }

    private void atomicEquality(String patternA, String patternB, boolean expectation, TransactionOLTP tx){
        atomicEquivalence(patternA, patternB, expectation, AtomicEquivalence.Equality, tx);
    }

    private void atomicEquivalence(Atomic a, Atomic b, boolean expectation, AtomicEquivalence equiv){
        assertEquals(equiv.name() + " Atomic: " + a.toString() + " =? " + b.toString(), expectation, equiv.equivalent(a, b));

        //check hash additionally if need to be equal
        if (expectation) {
            assertEquals(equiv.name() + " " + a.toString() + " hash=? " + b.toString(), true, equiv.hash(a) == equiv.hash(b));
        }
    }

    private Conjunction<Statement> conjunction(String patternString){
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }
}
