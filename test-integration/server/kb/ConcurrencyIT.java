package grakn.core.server.kb;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConcurrencyIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private TransactionOLTP tx;
    private SessionImpl session;

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
        tx = session.transaction().write();
    }

    @After
    public void tearDown() {
        tx.close();
        session.close();
    }


    @Test
    public void fun() throws ExecutionException, InterruptedException {
        tx.execute(Graql.parse("define " +
                "person sub entity, plays friend, has name; " +
                "friendship sub relation, relates friend; " +
                "name sub attribute, datatype string;").asDefine());

        tx.commit();
        ExecutorService executorService = Executors.newFixedThreadPool(8);

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                Random random = new Random();
                TransactionOLTP tx = session.transaction().write();
                for (int j = 0; j < 50; j++) {
                    String personId = tx.execute(Graql.parse("insert $x isa person, has name '" + random.nextInt() + "';").asInsert()).get(0).get("x").id().getValue();
                    String personId2 = tx.execute(Graql.parse("insert $x isa person, has name '" + random.nextInt() + "';").asInsert()).get(0).get("x").id().getValue();

                    tx.execute(Graql.parse("insert $x id "+personId+"; $y id "+ personId2 + "; (friend: $x, friend: $y) isa friendship;").asInsert());

                }
                tx.commit();

                return null;
            }, executorService);
            asyncInsertions.add(asyncInsert);
        }

        CompletableFuture.allOf(asyncInsertions.toArray(new CompletableFuture[]{})).get();
        long end = System.currentTimeMillis();
        tx = session.transaction().write();
        int size = tx.execute(Graql.parse("match $x isa person; get $x; count;").asGetAggregate()).get(0).number().intValue();
        int sizeNames = tx.execute(Graql.parse("match $x isa name; get $x; count;").asGetAggregate()).get(0).number().intValue();
        int sizeFriendships = tx.execute(Graql.parse("match $x isa friendship; get $x; count;").asGetAggregate()).get(0).number().intValue();

        System.out.println(size);
        System.out.println(sizeNames);
        System.out.println(sizeFriendships);
        System.out.println((end - start));
    }
}
