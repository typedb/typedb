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
public class TransitivityMatrixGraph {
    private final static Label key = Label.of("index");
    private final Session session;

    public TransitivityMatrixGraph(Session session) {
        this.session = session;
    }

    public void load(int n, int m) {
        loadSchema();
        buildExtensionalDB(n, m);
    }

    private void loadSchema() {
        try {
            InputStream inputStream = TransitivityMatrixGraph.class.getClassLoader().getResourceAsStream("test-integration/test/graql/reasoner/resources/quadraticTransitivity.gql");
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
        Role qfrom = tx.getRole("Q-from");
        Role qto = tx.getRole("Q-to");

        EntityType aEntity = tx.getEntityType("a-entity");
        RelationshipType q = tx.getRelationshipType("Q");
        Thing aInst = putEntityWithResource(tx, "a", tx.getEntityType("entity2"), key);
        ConceptId[][] aInstanceIds = new ConceptId[n][m];
        for(int i = 0 ; i < n ;i++) {
            for (int j = 0; j < m; j++) {
                aInstanceIds[i][j] = putEntityWithResource(tx, "a" + i + "," + j, aEntity, key).id();
            }
        }

        q.create()
                .assign(qfrom, aInst)
                .assign(qto, tx.getConcept(aInstanceIds[0][0]));

        for(int i = 0 ; i < n ; i++) {
            for (int j = 0; j < m ; j++) {
                if ( i < n - 1 ) {
                    q.create()
                            .assign(qfrom, tx.getConcept(aInstanceIds[i][j]))
                            .assign(qto, tx.getConcept(aInstanceIds[i+1][j]));
                }
                if ( j < m - 1){
                    q.create()
                            .assign(qfrom, tx.getConcept(aInstanceIds[i][j]))
                            .assign(qto, tx.getConcept(aInstanceIds[i][j+1]));
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
