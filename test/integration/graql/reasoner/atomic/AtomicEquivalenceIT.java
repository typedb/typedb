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
 */

package grakn.core.graql.reasoner.atomic;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import grakn.core.common.config.Config;
import grakn.core.core.AttributeValueConverter;
import grakn.core.graql.reasoner.atom.AtomicEquivalence;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestStorage;
import grakn.core.test.rule.SessionUtil;
import grakn.core.test.rule.TestTransactionProvider;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class AtomicEquivalenceIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session session;

    private Transaction tx;

    @BeforeClass
    public static void loadContext() {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        session = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);

        // define schema
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.parse("define " +
                    "organisation sub entity," +
                    "  has name, " +
                    "  plays employer," +
                    "  plays owner," +
                    "  abstract; " +
                    "company sub organisation;" +
                    "charity sub company;" +
                    "employment sub relation, " +
                    "  relates employer," +
                    "  relates employee;" +
                    "ownership sub relation," +
                    "  relates owner;" +
                    "name sub attribute, value string;").asDefine());

            // define an attribute type of each additional type
            tx.execute(Graql.parse("define " +
                    "age sub attribute, value long;" +
                    "volume sub attribute, value double;" +
                    "expiration sub attribute, value datetime;" +
                    "is-sold sub attribute, value boolean;").asDefine());
            tx.commit();
        }
    }

    @AfterClass
    public static void closeSession() {
        session.close();
    }

    @Before
    public void setUp() {
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @After
    public void tearDown() {
        tx.close();
    }

    @Test
    public void testEquality_DifferentIsaVariants() {
        testEquality_DifferentTypeVariants(tx, "isa", "company", "charity");
    }

    @Test
    public void testEquality_DifferentSubVariants() {
        testEquality_DifferentTypeVariants(tx, "sub", "company", "charity");
    }

    @Test
    public void testEquality_DifferentPlaysVariants() {
        testEquality_DifferentTypeVariants(tx, "plays", "employer", "employee");
    }

    @Test
    public void testEquality_DifferentRelatesVariants() {
        testEquality_DifferentTypeVariants(tx, "relates", "employer", "employee");
    }

    @Test
    public void testEquality_DifferentHasVariants() {
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ $x has name; };";
        String patternString2 = "{ $y has name; };";
        String patternString3 = "{ $x has " + Graql.Token.Type.ATTRIBUTE + "; };";

        atomicEquality(patternString, patternString, true, reasonerQueryFactory);
        atomicEquality(patternString, patternString2, false, reasonerQueryFactory);
        atomicEquality(patternString, patternString3, false, reasonerQueryFactory);
        atomicEquality(patternString2, patternString3, false, reasonerQueryFactory);
    }

    @Test
    public void testEquality_AttributeAtomsWithDifferingAttributeVariables() {
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ $x has name $v1; };";
        String patternString2 = "{ $x has name $v2; };";

        atomicEquality(patternString, patternString2, false, reasonerQueryFactory);
    }

    @Test
    public void testEquality_DifferentRelationVariants() {
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String pattern = "{ (employee: $x, employer: $y) isa employment; };";
        String directPattern = "{ (employee: $x, employer: $y) isa! employment; };";
        String pattern2 = "{ $r (employee: $x, employer: $y) isa employment; };";
        String pattern3 = "{ $z (employee: $x, employer: $y) isa employment; };";
        String pattern4 = "{ (employee: $x, employer: $y); };";
        String pattern5 = "{ (employee: $z, employer: $v) isa employment; };";
        String pattern6 = "{ (role: $x, employer: $y) isa employment; };";
        String pattern7 = "{ (employee: $x, employer: $y) isa $type; };";
        String pattern8 = "{ (employee: $x, employer: $y) isa $type;$type type employment; };";

        atomicEquality(pattern, pattern, true, reasonerQueryFactory);
        atomicEquality(pattern, directPattern, false, reasonerQueryFactory);
        atomicEquality(pattern, pattern2, false, reasonerQueryFactory);
        atomicEquality(pattern, pattern3, false, reasonerQueryFactory);
        atomicEquality(pattern, pattern4, true, reasonerQueryFactory); // true because of type inference!
        atomicEquality(pattern, pattern5, false, reasonerQueryFactory);
        atomicEquality(pattern, pattern6, false, reasonerQueryFactory);
        atomicEquality(pattern, pattern7, false, reasonerQueryFactory);
        atomicEquality(pattern, pattern8, false, reasonerQueryFactory);
    }

    private static Map<AttributeType.ValueType<?>, Object> testValues = ImmutableMap.<AttributeType.ValueType<?>, Object>builder()
            .put(AttributeType.ValueType.BOOLEAN, true)
            .put(AttributeType.ValueType.DATETIME, LocalDateTime.now())
            .put(AttributeType.ValueType.DOUBLE, 10.0)
            .put(AttributeType.ValueType.FLOAT, 10.0)
            .put(AttributeType.ValueType.INTEGER, 10)
            .put(AttributeType.ValueType.LONG, 10L)
            .put(AttributeType.ValueType.STRING, "10")
            .build();

    @Test
    public void testEquivalence_AttributesWithEquivalentValues() {
        AttributeType<?> metaAttributeType = tx.getMetaAttributeType();
        Set<AttributeType> attributeTypes = metaAttributeType.subs().collect(toSet());

        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        AttributeType.ValueType.values().stream()
                .filter(valueType -> attributeTypes.stream().anyMatch(t -> t.valueType() != null && t.valueType().equals(valueType)))
                .forEach(valueType -> {
                    Object value = testValues.get(valueType);
                    attributeTypes.stream()
                            .filter(t -> Objects.nonNull(t.valueType()))
                            .filter(t -> t.valueType().equals(valueType)).forEach(attributeType -> {

                        Pattern basePattern = Graql.parsePattern("$x has " + attributeType.label().getValue() + " " + value + ";");
                        valueType.comparableValueTypes().forEach(comparableValueType -> {
                            AttributeValueConverter<Object, ?> converter = AttributeValueConverter.of(comparableValueType);
                            Pattern convertedPattern = Graql.parsePattern("$x has " + attributeType.label().getValue() + " " + converter.convert(value) + ";");
                            atomicEquivalence(basePattern.toString(), convertedPattern.toString(), true, AtomicEquivalence.AlphaEquivalence, reasonerQueryFactory);
                            atomicEquivalence(basePattern.toString(), convertedPattern.toString(), true, AtomicEquivalence.StructuralEquivalence, reasonerQueryFactory);
                        });
                    });
                });
    }

    @Test
    public void testEquivalence_DifferentNonVariablePredicateVariants() {
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

        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        variablePredicates.forEach(vp -> {
            Conjunction<Statement> conj = (Conjunction<Statement>) Graql.and(vp);
            Atomic atom = reasonerQueryFactory.create(conj).getAtoms().stream().findFirst().orElse(null);
            Atomic atomCopy = atom.copy(atom.getParentQuery());

            atomicEquivalence(atom, atomCopy, true, AtomicEquivalence.Equality);
            atomicEquivalence(atom, atomCopy, true, AtomicEquivalence.AlphaEquivalence);
            atomicEquivalence(atom, atomCopy, true, AtomicEquivalence.StructuralEquivalence);
            variablePredicates.stream()
                    .filter(vp2 -> !vp.equals(vp2))
                    .forEach(vp2 -> {
                        Conjunction<Statement> conj2 = (Conjunction<Statement>) Graql.and(vp2);
                        Atomic atom2 = reasonerQueryFactory.create(conj2).getAtoms().stream().findFirst().orElse(null);

                        atomicEquivalence(atom, atom2, false, AtomicEquivalence.Equality);
                        atomicEquivalence(atom, atom2, false, AtomicEquivalence.AlphaEquivalence);
                        atomicEquivalence(atom, atom2, false, AtomicEquivalence.StructuralEquivalence);
                    });
        });
    }

    @Test
    public void testEquivalence_DifferentVariablePredicateVariants() {
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

        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        variablePredicates.forEach(vp -> {
            Conjunction<Statement> conj = (Conjunction<Statement>) Graql.and(vp);
            Atomic atom = reasonerQueryFactory.create(conj).getAtoms().stream().findFirst().orElse(null);
            Atomic atomCopy = atom.copy(atom.getParentQuery());
            assertEquals(atom, atomCopy);
            assertTrue(atom.isAlphaEquivalent(atomCopy));
            assertTrue(atom.isStructurallyEquivalent(atomCopy));
            variablePredicates.stream()
                    .filter(vp2 -> !vp.equals(vp2))
                    .forEach(vp2 -> {
                        Conjunction<Statement> conj2 = (Conjunction<Statement>) Graql.and(vp2);
                        Atomic atom2 = reasonerQueryFactory.create(conj2).getAtoms().stream().findFirst().orElse(null);
                        assertNotEquals("Unexpected equality outcome: " + atom + "==" + atom2, atom, atom2);
                        assertFalse("Unexpected alpha-equivalence outcome: " + atom + "==" + atom2, atom.isAlphaEquivalent(atom2));
                        assertFalse("Unexpected struct-equivalence outcome: " + atom + "==" + atom2, atom.isStructurallyEquivalent(atom2));
                    });
        });
    }

    @Test
    public void testEquality_AbstractId() {
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        // test for hash collisions AND equality check between atom produced by `isAbstract` and `type`
        String typeIsAbstract = Graql.var("x").type("organisation").toString();
        String isAbstract = Graql.var("x").isAbstract().toString();
        atomicEquality(typeIsAbstract, isAbstract, false, reasonerQueryFactory);
    }

    private void testEquality_DifferentTypeVariants(Transaction tx, String keyword, String label, String label2) {
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String variantAString = "{ $x " + keyword + " " + label + "; };";
        String variantAString2 = "{ $y " + keyword + " " + label + "; };";
        String variantAString3 = "{ $y " + keyword + " " + label2 + "; };";
        atomicEquality(variantAString, variantAString, true, reasonerQueryFactory);
        atomicEquality(variantAString, variantAString2, false, reasonerQueryFactory);
        atomicEquality(variantAString2, variantAString3, false, reasonerQueryFactory);

        String variantBString = "{ $x " + keyword + " $type; $type type " + label + "; };";
        String variantBString2 = "{ $x " + keyword + " $type; $type type " + label2 + "; };";
        String variantBString3 = "{ $x " + keyword + " $var; $var type " + label + "; };";
        String variantBString4 = "{ $y " + keyword + " $type; $type type " + label + "; };";
        atomicEquality(variantBString, variantBString, true, reasonerQueryFactory);
        atomicEquality(variantBString, variantBString2, false, reasonerQueryFactory);
        atomicEquality(variantBString, variantBString3, true, reasonerQueryFactory);
        atomicEquality(variantBString, variantBString4, false, reasonerQueryFactory);

        String variantCString = "{ $x " + keyword + " $y; };";
        String variantCString2 = "{ $x " + keyword + " $z; };";
        atomicEquality(variantCString, variantCString, true, reasonerQueryFactory);
        atomicEquality(variantCString, variantCString2, true, reasonerQueryFactory);

        atomicEquality(variantAString, variantBString, true, reasonerQueryFactory);
        atomicEquality(variantAString, variantCString, false, reasonerQueryFactory);
        atomicEquality(variantBString, variantCString, false, reasonerQueryFactory);
    }

    private void atomicEquivalence(String patternA, String patternB, boolean expectation, AtomicEquivalence equiv, ReasonerQueryFactory reasonerQueryFactory) {
        Atomic atomA = Iterables.getOnlyElement(reasonerQueryFactory.create(conjunction(patternA)).getAtoms());
        Atomic atomB = Iterables.getOnlyElement(reasonerQueryFactory.create(conjunction(patternB)).getAtoms());
        atomicEquivalence(atomA, atomA, true, equiv);
        atomicEquivalence(atomB, atomB, true, equiv);
        atomicEquivalence(atomA, atomB, expectation, equiv);
        atomicEquivalence(atomB, atomA, expectation, equiv);
    }

    private void atomicEquality(String patternA, String patternB, boolean expectation, ReasonerQueryFactory reasonerQueryFactory) {
        atomicEquivalence(patternA, patternB, expectation, AtomicEquivalence.Equality, reasonerQueryFactory);
    }

    private void atomicEquivalence(Atomic a, Atomic b, boolean expectation, AtomicEquivalence equiv) {
        assertEquals(equiv.name() + " Atomic: " + a.toString() + " =? " + b.toString(), expectation, equiv.equivalent(a, b));

        //check hash additionally if need to be equal
        if (expectation) {
            assertEquals(equiv.name() + " " + a.toString() + " hash=? " + b.toString(), true, equiv.hash(a) == equiv.hash(b));
        }
    }

    private Conjunction<Statement> conjunction(String patternString) {
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }
}
