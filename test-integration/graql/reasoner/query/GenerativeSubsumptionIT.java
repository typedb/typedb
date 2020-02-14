package grakn.core.graql.reasoner.query;

import grakn.common.util.Pair;
import grakn.core.common.config.Config;
import grakn.core.graql.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.statement.Statement;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GenerativeSubsumptionIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session genericSchemaSession;

    @BeforeClass
    public static void loadContext() {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        genericSchemaSession = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        String resourcePath = "test-integration/graql/reasoner/resources/";
        loadFromFileAndCommit(resourcePath, "genericSchema.gql", genericSchemaSession);
    }

    @AfterClass
    public static void closeSession() {
        genericSchemaSession.close();
    }

    //TODO we currently precompute the pairs using the operator test tools, once they are public, generate the pairs in place
    private static List<Pair<String, String>> readPairs() {
        String path = "test-integration/graql/reasoner/resources/";
        String fileName = "generatedSubsumptionPairs";
        //String fileName = "generatedSubsumptionPairsFull";
        String delim = " -> ";

        List<Pair<String, String>> pairs = new ArrayList<>();
        try (InputStream inputStream = GenerativeSubsumptionIT.class.getClassLoader().getResourceAsStream(path + fileName)) {
            List<String> lines = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.toList());
            for(String line : lines){
                String[] patterns = line.split(delim);

                String parent = patterns[0];
                String child = patterns[1];
                pairs.add(new Pair<>(parent, child));
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return pairs;
    }

    @Test
    public void testSubsumptionRelationHoldsBetweenGeneratedPairs(){
        List<Pair<String, String>> pairs = readPairs();
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction) tx).reasonerQueryFactory();
            String id = tx.getEntityType("baseRoleEntity").instances().iterator().next().id().getValue();
            String subId = tx.getEntityType("subRoleEntity").instances().iterator().next().id().getValue();

            boolean pass = true;
            int failures = 0;
            int processed = 0;
            for(Pair<String, String> pair : pairs){
                Pair<String, String> cPair = contextualiseIds(pair, id, subId);

                ReasonerQueryImpl pQuery = reasonerQueryFactory.create(conjunction(cPair.first()));
                ReasonerQueryImpl cQuery = reasonerQueryFactory.create(conjunction(cPair.second()));

                if (pQuery.isAtomic() && cQuery.isAtomic()) {
                    ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(pQuery.getAtoms());
                    ReasonerAtomicQuery child = reasonerQueryFactory.atomic(cQuery.getAtoms());

                    if(!parent.isSubsumedBy(child)){
                        //System.out.println("Subsumption failure comparing : " + parent + " ?=< " + child);
                        pass = false;
                        //failures++;
                    }
                    if(parent.getMultiUnifier(child, UnifierType.RULE).equals(MultiUnifierImpl.nonExistent())){
                        //System.out.println("Unifier failure comparing : " + parent + " ?=< " + child);
                        pass = false;
                        failures++;
                    }
                }
                processed++;
                if (processed % 5000 == 0) System.out.println("failures: " + failures + "/" + processed);
            }
            System.out.println("failures: " + failures);
            assertTrue(pass);
        }
    }

    private Pair<String, String> contextualiseIds(Pair<String, String> pair, String id, String subId){
        String placeholderId = "V123";
        String subPlaceholderId = "V456";
        return new Pair<>(
                pair.first().replaceAll(placeholderId, id).replaceAll(subPlaceholderId, subId),
                pair.second().replaceAll(placeholderId, id).replaceAll(subPlaceholderId, subId)
        );
    }

    private Conjunction<Statement> conjunction(String patternString) {
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }
}
