package grakn.core.graql.internal.reasoner.graph;

import grakn.core.Session;
import grakn.core.Transaction;
import grakn.core.concept.Attribute;
import grakn.core.concept.AttributeType;
import grakn.core.concept.ConceptId;
import grakn.core.concept.EntityType;
import grakn.core.concept.Label;
import grakn.core.concept.RelationshipType;
import grakn.core.concept.Role;
import grakn.core.concept.Thing;
import grakn.core.graql.Query;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

@SuppressWarnings("CheckReturnValue")
public class DiagonalGraph {
    private final static Label key = Label.of("name");
    private final Session session;

    public DiagonalGraph(Session session) {
        this.session = session;
    }

    public void load(int n, int m) {
        loadSchema();
        buildExtensionalDB(n, m);
    }

    private void loadSchema() {
        try {
            InputStream inputStream = DiagonalGraph.class.getClassLoader().getResourceAsStream("test-integration/test/graql/reasoner/resources/diagonalTest.gql");
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            tx.graql().parser().parseList(s).forEach(Query::execute);
            tx.commit();
        } catch (Exception e) {
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    private void buildExtensionalDB(int n, int m) {
        Transaction tx = session.transaction(Transaction.Type.WRITE);
        Role relFrom = tx.getRole("rel-from");
        Role relTo = tx.getRole("rel-to");

        EntityType entity1 = tx.getEntityType("entity1");
        RelationshipType horizontal = tx.getRelationshipType("horizontal");
        RelationshipType vertical = tx.getRelationshipType("vertical");
        ConceptId[][] instanceIds = new ConceptId[n][m];
        long inserts = 0;
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                instanceIds[i][j] = putEntityWithResource(tx, "a" + i + "," + j, entity1, key).id();
                inserts++;
                if (inserts % 100 == 0) System.out.println("inst inserts: " + inserts);

            }
        }

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                if (i < n - 1) {
                    vertical.create()
                            .assign(relFrom, tx.getConcept(instanceIds[i][j]))
                            .assign(relTo, tx.getConcept(instanceIds[i + 1][j]));
                    inserts++;
                }
                if (j < m - 1) {
                    horizontal.create()
                            .assign(relFrom, tx.getConcept(instanceIds[i][j]))
                            .assign(relTo, tx.getConcept(instanceIds[i][j + 1]));
                    inserts++;
                }
                if (inserts % 100 == 0) System.out.println("rel inserts: " + inserts);
            }
        }
        System.out.println("Extensional DB loaded.");
        tx.commit();
    }


    private static Thing putEntityWithResource(Transaction tx, String id, EntityType type, Label key) {
        Thing inst = type.create();
        putResource(inst, tx.getSchemaConcept(key), id);
        return inst;
    }

    private static <T> void putResource(Thing thing, AttributeType<T> attributeType, T resource) {
        Attribute attributeInstance = attributeType.create(resource);
        thing.has(attributeInstance);
    }
}
