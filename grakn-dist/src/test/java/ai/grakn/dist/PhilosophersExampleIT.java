package ai.grakn.dist;

import ai.grakn.Grakn;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.GraqlTestUtil.assertExists;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;

public class PhilosophersExampleIT {

    private static QueryBuilder qb;

    @BeforeClass
    public static void setUp() throws IOException {
        GraknTx tx = Grakn.session("in-memory", "mygraph").open(GraknTxType.WRITE);
        qb = tx.graql();
        runQueries("src/examples/philosophers.gql");
    }


    @Test
    public void testAlexanderHasShahOfPersiaTitle() {
        assertExists(qb, var().has("name", "Alexander").has("title", "Shah of Persia"));

    }

    @Test
    public void testThereAreFourPeople() {
        assertEquals(4, qb.<GetQuery>parse("match $p isa person; get;").stream().count());
    }

    @Test
    public void testCynicismIsInTheKB() {
        assertEquals(1, qb.<GetQuery>parse("match $x has name 'Cynicism'; get;").stream().count());
    }

    @Test
    public void testThereAreTwoPhilosophersPracticingPlatonism() {
        assertEquals(
                2,
                qb.<GetQuery>parse("match (philosopher: $x, $platonism) isa practice; $platonism has name 'Platonism'; get;").stream().count()
        );
    }

    @Test
    public void testAlexanderIsTheOnlyPharaoh() {
        GetQuery pharaoh = qb.parse("match has name $x, has title contains 'Pharaoh'; get;");
        assertEquals("Alexander", pharaoh.iterator().next().get("x").asAttribute().getValue());
    }

    @Test
    public void testSocratesKnowsTwoThings() {
        assertEquals(
                2,
                qb.<GetQuery>parse("match $socrates has name 'Socrates'; ($socrates, $x) isa knowledge; get;").stream().count()
        );
    }

    private static void runQueries(String path) throws IOException {
        String query = Files.readAllLines(Paths.get(path)).stream().collect(joining("\n"));
        qb.parser().parseList(query).forEach(Query::execute);
    }

}
