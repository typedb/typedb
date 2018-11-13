package grakn.core.graql.reasoner.graph;

import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.Query;
import com.google.common.math.IntMath;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

/**
 * Defines a Graph based on test 6.10 from Cao p. 82.
 */
@SuppressWarnings("CheckReturnValue")
public class PathTreeGraph {
    private final static Label key = Label.of("index");
    private final Session session;

    public PathTreeGraph(Session session) {
        this.session = session;
    }

    public void load(int n, int children) {
        loadSchema();
        buildExtensionalDB(n, children);
    }

    private void loadSchema() {
        try {
            InputStream inputStream = PathTreeGraph.class.getClassLoader().getResourceAsStream("test-integration/graql/reasoner/resources/pathTest.gql");
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            tx.graql().parser().parseList(s).forEach(Query::execute);
            tx.commit();
        } catch (Exception e) {
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    private void buildExtensionalDB(int n, int children){
        Transaction tx = session.transaction(Transaction.Type.WRITE);
        Role fromRole = tx.getRole("arc-from");
        Role toRole = tx.getRole("arc-to");
        long startTime = System.currentTimeMillis();

        EntityType vertex = tx.getEntityType("vertex");
        EntityType startVertex = tx.getEntityType("start-vertex");

        RelationshipType arc = tx.getRelationshipType("arc");
        putEntityWithResource(tx, "a0", startVertex, key);

        int outputThreshold = 500;
        for (int i = 1; i <= n; i++) {
            int m = IntMath.pow(children, i);
            for (int j = 0; j < m; j++) {
                putEntityWithResource(tx, "a" + i + "," + j, vertex, key);
                if (j != 0 && j % outputThreshold == 0) {
                    System.out.println(j + " entities out of " + m + " inserted");
                }
            }
        }

        for (int j = 0; j < children; j++) {
            arc.create()
                    .assign(fromRole, getInstance(tx, "a0"))
                    .assign(toRole, getInstance(tx, "a1," + j));
        }

        for (int i = 1; i < n; i++) {
            int m = IntMath.pow(children, i);
            for (int j = 0; j < m; j++) {
                for (int c = 0; c < children; c++) {
                    arc.create()
                            .assign(fromRole, getInstance(tx, "a" + i + "," + j))
                            .assign(toRole, getInstance(tx, "a" + (i + 1) + "," + (j * children + c)));

                }
                if (j != 0 && j % outputThreshold == 0) {
                    System.out.println("level " + i + "/" + (n - 1) + ": " + j + " entities out of " + m + " connected");
                }
            }
        }

        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("PathKB loading time: " + loadTime + " ms");
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

    private static Thing getInstance(Transaction tx, String id){
        Set<Thing> things = tx.getAttributesByValue(id)
                .stream().flatMap(Attribute::owners).collect(toSet());
        if (things.size() != 1) {
            throw new IllegalStateException("Multiple things with given resource value");
        }
        return things.iterator().next();
    }
}
