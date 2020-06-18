package grakn.core.test.behaviour.resolution.complete;

import grakn.client.GraknClient;
import grakn.client.GraknClient.Session;
import grakn.client.GraknClient.Transaction;
import grakn.client.answer.ConceptMap;
import grakn.client.concept.Rule;
import grakn.client.concept.thing.Thing;
import grakn.client.concept.type.AttributeType;
import grakn.client.concept.type.RelationType;
import grakn.client.concept.type.Role;
import grakn.client.concept.type.Type;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.verification.resolution.common.Utils.loadGqlFile;

public class SchemaManager {
    private static final Path SCHEMA_PATH = Paths.get("resolution", "complete", "completion_schema.gql").toAbsolutePath();

    private static HashSet<String> EXCLUDED_ENTITY_TYPES = new HashSet<String>() {
        {
            add("entity");
        }
    };

    private static HashSet<String> EXCLUDED_RELATION_TYPES = new HashSet<String>() {
        {
            add("relation");
            add("var-property");
            add("isa-property");
            add("has-attribute-property");
            add("relation-property");
            add("resolution");
        }
    };

    private static HashSet<String> EXCLUDED_ATTRIBUTE_TYPES = new HashSet<String>() {
        {
            add("attribute");
            add("label");
            add("rule-label");
            add("type-label");
            add("role-label");
        }
    };

    public static void undefineAllRules(Session session) {
        try (Transaction tx = session.transaction().write()) {
            Set<String> ruleLabels = getAllRules(tx).stream().map(rule -> rule.label().toString()).collect(Collectors.toSet());
            for (String ruleLabel : ruleLabels) {
                tx.execute(Graql.undefine(Graql.type(ruleLabel).sub("rule")));
            }
            tx.commit();
        }
    }

    public static Set<Rule> getAllRules(Transaction tx) {
        return tx.stream(Graql.match(Graql.var("r").sub("rule")).get()).map(ans -> ans.get("r").asRule()).filter(rule -> !rule.label().toString().equals("rule")).collect(Collectors.toSet());
    }

    public static void addResolutionSchema(Session session) {
        try {
            loadGqlFile(session, SCHEMA_PATH);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Role getRole(Transaction tx, String roleLabel) {
        GraqlGet roleQuery = Graql.match(Graql.var("x").sub(roleLabel)).get();
        return tx.execute(roleQuery).get(0).get("x").asRole();
    }

    public static void connectResolutionSchema(Session session) {
        try (Transaction tx = session.transaction().write()) {
            Role instanceRole = getRole(tx, "instance");
            Role ownerRole = getRole(tx, "owner");
            Role roleplayerRole = getRole(tx, "roleplayer");
            Role relRole = getRole(tx, "rel");

            RelationType attrPropRel = tx.execute(Graql.match(Graql.var("x").sub("has-attribute-property")).get()).get(0).get("x").asRelationType();
            
            GraqlGet typesToConnectQuery = Graql.match(
                    Graql.var("x").sub("thing"),
                    Graql.not(Graql.var("x").sub("@has-attribute")),
                    Graql.not(Graql.var("x").sub("@key-attribute"))
            ).get();
            tx.stream(typesToConnectQuery).map(ans -> ans.get("x").asType()).forEach(type -> {
                if (type.isAttributeType()) {
                    if (!EXCLUDED_ATTRIBUTE_TYPES.contains(type.label().toString())) {
                        attrPropRel.asRemote(tx).has((AttributeType) type);
                    }
                } else if (type.isEntityType()) {
                    if (!EXCLUDED_ENTITY_TYPES.contains(type.label().toString())) {
                        type.asRemote(tx).plays(instanceRole);
                        type.asRemote(tx).plays(ownerRole);
                        type.asRemote(tx).plays(roleplayerRole);
                    }

                } else if (type.isRelationType()) {
                    if (!EXCLUDED_RELATION_TYPES.contains(type.label().toString())) {
                        type.asRemote(tx).plays(instanceRole);
                        type.asRemote(tx).plays(ownerRole);
                        type.asRemote(tx).plays(roleplayerRole);
                        type.asRemote(tx).plays(relRole);
                    }
                }
            });
            tx.commit();
        }
    }

    public static void enforceAllTypesHaveKeys(Session session) {
        Transaction tx = session.transaction().read();

        GraqlGet instancesQuery = Graql.match(Graql.var("x").sub("thing"),
                Graql.not(Graql.var("x").sub("@has-attribute")),
                Graql.not(Graql.var("x").sub("@key-attribute")),
                Graql.not(Graql.var("x").sub("attribute")),
                Graql.not(Graql.var("x").type("entity")),
                Graql.not(Graql.var("x").type("relation")),
                Graql.not(Graql.var("x").type("thing"))
        ).get();
        Stream<ConceptMap> answers = tx.stream(instancesQuery);

        answers.forEach(ans -> {
            Type<?, ?> type = ans.get("x").asType();
            Type.Remote<?, ?> remoteType = type.asRemote(tx);
            if (!remoteType.isAbstract() && remoteType.keys().collect(Collectors.toSet()).isEmpty()) {
                throw new RuntimeException(String.format("Type \"%s\" doesn't have any keys declared. Keys are required " +
                        "for all entity types and relation types for resolution testing", type.label().toString()));
            }
        });
        tx.close();
    }
}
