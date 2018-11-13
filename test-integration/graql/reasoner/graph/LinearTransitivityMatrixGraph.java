package grakn.core.graql.reasoner.graph;

import grakn.core.server.Session;
import grakn.core.server.Transaction;
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
public class LinearTransitivityMatrixGraph {

    private final static Label key = Label.of("index");
    private final Session session;

    public LinearTransitivityMatrixGraph(Session session) {
        this.session = session;
    }

    public void load(int n, int m) {
        loadSchema();
        buildExtensionalDB(n, m);
    }

    private void loadSchema() {
        try {
            InputStream inputStream = LinearTransitivityMatrixGraph.class.getClassLoader().getResourceAsStream("test-integration/graql/reasoner/resources/linearTransitivity.gql");
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            tx.graql().parser().parseList(s).forEach(Query::execute);
            tx.commit();
        } catch (Exception e) {
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    private void buildExtensionalDB(int n, int m){
        Transaction tx = session.transaction(Transaction.Type.WRITE);
        Role Qfrom = tx.getRole("Q-from");
        Role Qto = tx.getRole("Q-to");

        EntityType aEntity = tx.getEntityType("a-entity");
        RelationshipType Q = tx.getRelationshipType("Q");
        ConceptId[][] aInstancesIds = new ConceptId[n+1][m+1];
        Thing aInst = putEntityWithResource(tx, "a", tx.getEntityType("entity2"), key);
        for(int i = 1 ; i <= n ;i++) {
            for (int j = 1; j <= m; j++) {
                aInstancesIds[i][j] = putEntityWithResource(tx, "a" + i + "," + j, aEntity, key).id();
            }
        }

        Q.create()
                .assign(Qfrom, aInst)
                .assign(Qto, tx.getConcept(aInstancesIds[1][1]));

        for(int i = 1 ; i <= n ; i++) {
            for (int j = 1; j <= m; j++) {
                if ( i < n ) {
                    Q.create()
                            .assign(Qfrom, tx.getConcept(aInstancesIds[i][j]))
                            .assign(Qto, tx.getConcept(aInstancesIds[i+1][j]));
                }
                if ( j < m){
                    Q.create()
                            .assign(Qfrom, tx.getConcept(aInstancesIds[i][j]))
                            .assign(Qto, tx.getConcept(aInstancesIds[i][j+1]));
                }
            }
        }
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
