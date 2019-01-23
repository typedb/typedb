package grakn.core.graql.reasoner.graph;

import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.server.Session;
import grakn.core.server.Transaction;

import static grakn.core.util.GraqlTestUtil.getInstance;
import static grakn.core.util.GraqlTestUtil.loadFromFile;
import static grakn.core.util.GraqlTestUtil.putEntityWithResource;

/**
 * Defines a KB for the the following tests:
 * - 5.2 from Green
 * - 6.16 from Cao
 * - 6.18 from Cao
 */
public class ReachabilityGraph {

    private final Session session;
    private final static String gqlPath = "test-integration/graql/reasoner/resources/recursion/";
    private final static String gqlFile = "reachability.gql";
    private final static Label key = Label.of("index");

    public ReachabilityGraph(Session session){
        this.session = session;
    }

    public final void load(int n) {
        Transaction tx = session.transaction(Transaction.Type.WRITE);
        loadFromFile(gqlPath, gqlFile, tx);
        buildExtensionalDB(n, tx);
        tx.commit();
    }

    protected void buildExtensionalDB(int n, Transaction tx) {
        EntityType vertex = tx.getEntityType("vertex");
        Role from = tx.getRole("from");
        Role to = tx.getRole("to");
        RelationType link = tx.getRelationshipType("link");

        //basic test variant from Green
        putEntityWithResource(tx, "aa", vertex, key);
        putEntityWithResource(tx, "bb", vertex, key);
        putEntityWithResource(tx, "cc", vertex, key);
        putEntityWithResource(tx, "dd", vertex, key);
        link.create().assign(from, getInstance(tx, "aa")).assign(to, getInstance(tx, "bb"));
        link.create().assign(from, getInstance(tx, "bb")).assign(to, getInstance(tx, "cc"));
        link.create().assign(from, getInstance(tx, "cc")).assign(to, getInstance(tx, "cc"));
        link.create().assign(from, getInstance(tx, "cc")).assign(to, getInstance(tx, "dd"));

        //EDB for Cao tests
        putEntityWithResource(tx, "a", vertex, key);
        for(int i = 1 ; i <=n ; i++) {
            putEntityWithResource(tx, "a" + i, vertex, key);
            putEntityWithResource(tx, "b" + i, vertex, key);
        }

        link.create()
                .assign(from, getInstance(tx, "a"))
                .assign(to, getInstance(tx, "a1"));
        link.create()
                .assign(from, getInstance(tx, "a" + n))
                .assign(to, getInstance(tx, "a1"));
        link.create()
                .assign(from, getInstance(tx, "b" + n))
                .assign(to, getInstance(tx, "b1"));

        for(int i = 1 ; i < n ; i++) {
            link.create()
                    .assign(from, getInstance(tx, "a" + i))
                    .assign(to, getInstance(tx, "a" + (i + 1)));
            link.create()
                    .assign(from, getInstance(tx, "b" + i))
                    .assign(to, getInstance(tx, "b" + (i + 1)));
        }
    }
}

