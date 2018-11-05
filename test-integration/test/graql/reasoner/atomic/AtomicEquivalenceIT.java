package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.graql.Query;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.rule.ConcurrentGraknServer;
import ai.grakn.util.Schema;
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

@SuppressWarnings("CheckReturnValue")
public class AtomicEquivalenceIT {

    @ClassRule
    public static final ConcurrentGraknServer server = new ConcurrentGraknServer();

    private static EmbeddedGraknSession genericSchemaSession;

    private static void loadFromFile(String fileName, GraknSession session){
        try {
            InputStream inputStream = AtomicEquivalenceIT.class.getClassLoader().getResourceAsStream("test-integration/test/graql/reasoner/resources/"+fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            GraknTx tx = session.transaction(GraknTxType.WRITE);
            tx.graql().parser().parseList(s).forEach(Query::execute);
            tx.commit();
        } catch (Exception e){
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    private EmbeddedGraknTx tx;

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
        tx = genericSchemaSession.transaction(GraknTxType.WRITE);
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
        String patternString = "{$x has resource;}";
        String patternString2 = "{$y has resource;}";
        String patternString3 = "{$x has " + Schema.MetaSchema.ATTRIBUTE.getLabel().getValue() + ";}";

        atomicEquality(patternString, patternString, true, tx);
        atomicEquality(patternString, patternString2, false, tx);
        atomicEquality(patternString, patternString3, false, tx);
        atomicEquality(patternString2, patternString3, false, tx);
    }

    @Test
    public void testEquivalence_DifferentRelationVariants(){
        String pattern = "{(baseRole1: $x, baseRole2: $y) isa binary;}";
        String directPattern = "{(baseRole1: $x, baseRole2: $y) isa! binary;}";
        String pattern2 = "{$r (baseRole1: $x, baseRole2: $y) isa binary;}";
        String pattern3 = "{$z (baseRole1: $x, baseRole2: $y) isa binary;}";
        String pattern4 = "{(baseRole1: $x, baseRole2: $y);}";
        String pattern5 = "{(baseRole1: $z, baseRole2: $v) isa binary;}";
        String pattern6 = "{(role: $x, baseRole2: $y) isa binary;}";
        String pattern7 = "{(baseRole1: $x, baseRole2: $y) isa $type;}";
        String pattern8 = "{(baseRole1: $x, baseRole2: $y) isa $type;$type label binary;}";

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

    private void testEquality_DifferentTypeVariants(EmbeddedGraknTx<?> tx, String keyword, String label, String label2){
        String variantAString = "{$x " + keyword + " " + label + ";}";
        String variantAString2 = "{$y " + keyword + " " + label + ";}";
        String variantAString3 = "{$y " + keyword + " " + label2 + ";}";
        atomicEquality(variantAString, variantAString, true, tx);
        atomicEquality(variantAString, variantAString2, false, tx);
        atomicEquality(variantAString2, variantAString3, false, tx);

        String variantBString = "{$x " + keyword + " $type;$type label " + label +";}";
        String variantBString2 = "{$x " + keyword + " $type;$type label " + label2 +";}";
        String variantBString3 = "{$x " + keyword + " $var;$var label " + label +";}";
        String variantBString4 = "{$y " + keyword + " $type;$type label " + label +";}";
        atomicEquality(variantBString, variantBString, true, tx);
        atomicEquality(variantBString, variantBString2, false, tx);
        atomicEquality(variantBString, variantBString3, true, tx);
        atomicEquality(variantBString, variantBString4, false, tx);

        String variantCString = "{$x " + keyword + " $y;}";
        String variantCString2 = "{$x " + keyword + " $z;}";
        atomicEquality(variantCString, variantCString, true, tx);
        atomicEquality(variantCString, variantCString2, true, tx);

        atomicEquality(variantAString, variantBString, true, tx);
        atomicEquality(variantAString, variantCString, false, tx);
        atomicEquality(variantBString, variantCString, false, tx);
    }

    private void atomicEquality(String patternA, String patternB, boolean expectation, EmbeddedGraknTx<?> tx){
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

    private Conjunction<VarPatternAdmin> conjunction(String patternString, EmbeddedGraknTx<?> tx){
        Set<VarPatternAdmin> vars = tx.graql().parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}
