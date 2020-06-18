package grakn.core.test.behaviour.resolution.test;

import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.behaviour.resolution.complete.SchemaManager;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.test.behaviour.resolution.common.Utils.loadGqlFile;
import static grakn.core.test.behaviour.resolution.complete.SchemaManager.addResolutionSchema;
import static grakn.core.test.behaviour.resolution.complete.SchemaManager.connectResolutionSchema;
import static org.junit.Assert.assertEquals;

public class TestSchemaManager {

    private static final String KEYSPACE = "test_schema_manager";

    @ClassRule
    public static final GraknTestServer graknTestServer = new GraknTestServer();

    @BeforeClass
    public static void beforeClass() {
        try (Session session = graknTestServer.session(KEYSPACE)) {
            try {
                Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case2", "schema.gql").toAbsolutePath();
                loadGqlFile(session, schemaPath);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    @Test
    public void testResolutionSchemaRolesPlayedAreCorrect() {

        try (Session session = graknTestServer.session(KEYSPACE)) {

            addResolutionSchema(session);
            connectResolutionSchema(session);

            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {

                GraqlGet roleplayersQuery = Graql.match(
                        Graql.var("x").plays("instance"),
                        Graql.var("x").plays("owner"),
                        Graql.var("x").plays("roleplayer")
                ).get();

                Set<String> roleplayers = tx.stream(roleplayersQuery).map(r -> r.get("x").asType().label().toString()).collect(Collectors.toSet());

                HashSet<String> expectedRoleplayers = new HashSet<String>() {
                    {
                        add("transaction");
                        add("locates");
                        add("location-hierarchy");
                        add("location");
                        add("country");
                        add("city");
                    }
                };

                assertEquals(expectedRoleplayers, roleplayers);
            }
        }

    }

    @Test
    public void testResolutionSchemaRelationRolePlayedIsCorrect() {

        try (Session session = graknTestServer.session(KEYSPACE)) {

            addResolutionSchema(session);
            connectResolutionSchema(session);

            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {

                GraqlGet roleplayersQuery = Graql.match(
                        Graql.var("x").plays("rel")
                ).get();

                Set<String> roleplayers = tx.stream(roleplayersQuery).map(r -> r.get("x").asType().label().toString()).collect(Collectors.toSet());

                HashSet<String> expectedRoleplayers = new HashSet<String>() {
                    {
                        add("locates");
                        add("location-hierarchy");
                    }
                };
                assertEquals(expectedRoleplayers, roleplayers);
            }
        }

    }

    @Test
    public void testResolutionSchemaAttributesOwnedAreCorrect() {

        try (Session session = graknTestServer.session(KEYSPACE)) {

            addResolutionSchema(session);
            connectResolutionSchema(session);

            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {

                GraqlGet clauseAttributesQuery = Graql.match(Graql.var("x").sub("has-attribute-property")).get();

                Set<String> attributeTypes = tx.execute(clauseAttributesQuery).get(0).get("x").asRelationType().has().map(a -> a.label().toString()).collect(Collectors.toSet());

                HashSet<String> expectedAttributeTypes = new HashSet<String>() {
                    {
                        add("currency");
                        add("location-id");
                        add("transaction-id");
                        add("hierarchy-id");
                        add("country-name");
                        add("city-name");
                    }
                };

                assertEquals(expectedAttributeTypes, attributeTypes);
            }
        }
    }

    @Test
    public void testGetAllRulesReturnsExpectedRules() {
        try (Session session = graknTestServer.session(KEYSPACE)) {
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                Set<Rule> rules = SchemaManager.getAllRules(tx);
                assertEquals(2, rules.size());

                HashSet<String> expectedRuleLabels = new HashSet<String>(){
                    {
                        add("transaction-currency-is-that-of-the-country");
                        add("locates-is-transitive");
                    }
                };
                assertEquals(expectedRuleLabels, rules.stream().map(rule -> rule.label().toString()).collect(Collectors.toSet()));
            }
        }
    }

    @Test
    public void testUndefineAllRulesSuccessfullyUndefinesAllRules() {
        try (Session session = graknTestServer.session(KEYSPACE)) {
            SchemaManager.undefineAllRules(session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                List<String> ruleLabels = tx.stream(Graql.match(Graql.var("x").sub("rule")).get("x")).map(ans -> ans.get("x").asRule().label().toString()).collect(Collectors.toList());
                assertEquals(1, ruleLabels.size());
                assertEquals("rule", ruleLabels.get(0));
            }
        }
    }
}
