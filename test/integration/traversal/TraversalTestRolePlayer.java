package grakn.core.test.integration.traversal;

import grakn.core.Grakn;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Label;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.procedure.GraphProcedure;
import grakn.core.traversal.procedure.ProcedureVertex;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.parameters.Arguments.Transaction.Type.READ;
import static grakn.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static org.junit.Assert.assertTrue;

public class TraversalTestRolePlayer {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("query-test");
    private static String database = "query-test";

    private static RocksGrakn grakn;
    private static RocksSession session;

    @BeforeClass
    public static void setup() throws IOException {
        Util.resetDirectory(directory);
        grakn = RocksGrakn.open(directory);
        grakn.databases().create(database);
        session = grakn.session(database, Arguments.Session.Type.SCHEMA);
        try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
            GraqlDefine query = Graql.parseQuery("       define\n" +
                                                         "      person sub entity,\n" +
                                                         "        plays friendship:friend,\n" +
                                                         "        plays employment:employee,\n" +
                                                         "        owns name,\n" +
                                                         "        owns age,\n" +
                                                         "        owns ref @key;\n" +
                                                         "      company sub entity,\n" +
                                                         "        plays employment:employer,\n" +
                                                         "        owns name,\n" +
                                                         "        owns ref @key;\n" +
                                                         "      friendship sub relation,\n" +
                                                         "        relates friend,\n" +
                                                         "        owns ref @key;\n" +
                                                         "      employment sub relation,\n" +
                                                         "        relates employee,\n" +
                                                         "        relates employer,\n" +
                                                         "        owns ref @key;\n" +
                                                         "      name sub attribute, value string;\n" +
                                                         "      age sub attribute, value long;\n" +
                                                         "      ref sub attribute, value long;");
            transaction.query().define(query);
            query = Graql.parseQuery("define\n" +
                                             "\n" +
                                             "      gift-delivery sub relation,\n" +
                                             "        relates sender,\n" +
                                             "        relates recipient;\n" +
                                             "\n" +
                                             "      person plays gift-delivery:sender,\n" +
                                             "        plays gift-delivery:recipient;");
            transaction.query().define(query);
            transaction.commit();
        }

        session.close();
        session = grakn.session(database, Arguments.Session.Type.DATA);
        try (Grakn.Transaction tx = session.transaction(WRITE)) {
            tx.query().insert(Graql.parseQuery("      insert\n" +
                                                       "      $x1 isa person, has name \"Soroush\", has ref 0;\n" +
                                                       "      $x2a isa person, has name \"Martha\", has ref 1;\n" +
                                                       "      $x2b isa person, has name \"Patricia\", has ref 2;\n" +
                                                       "      $x2c isa person, has name \"Lily\", has ref 3;\n" +
                                                       "\n" +
                                                       "      (sender: $x1, recipient: $x2a) isa gift-delivery;\n" +
                                                       "      (sender: $x1, recipient: $x2b) isa gift-delivery;\n" +
                                                       "      (sender: $x1, recipient: $x2c) isa gift-delivery;\n" +
                                                       "      (sender: $x2a, recipient: $x2b) isa gift-delivery;"));
            tx.commit();
        }
    }

    @AfterClass
    public static void teardown() {
        grakn.close();
    }


    @Test
    public void roleplayer_test() {

        try (RocksTransaction transaction = session.transaction(READ)) {
            GraphProcedure.Builder proc = GraphProcedure.builder(4);
            Traversal.Parameters params = new Traversal.Parameters();

            ProcedureVertex.Thing _0 = proc.anonymousThing(0);
            _0.props().types(set(Label.of("gift-delivery")));
            ProcedureVertex.Thing _1 = proc.anonymousThing(1);
            _1.props().types(set(Label.of("gift-delivery")));
            ProcedureVertex.Thing a = proc.namedThing("a");
            a.props().types(set(Label.of("person")));
            ProcedureVertex.Thing b = proc.namedThing("b");
            b.props().types(set(Label.of("person")));
            ProcedureVertex.Thing c = proc.namedThing("c", true);
            c.props().types(set(Label.of("person")));

        /*
        2021-02-02 09:25:16,483 [main] [DEBUG] g.c.t.procedure.GraphProcedure - Graph Procedure: {
	vertices:
		$_0 [thing] { hasIID: false, types: [gift-delivery], predicates: [] }
		$_1 [thing] { hasIID: false, types: [gift-delivery], predicates: [] }
		$a [thing] { hasIID: false, types: [person], predicates: [] } (end)
		$b [thing] { hasIID: false, types: [person], predicates: [] }
		$c [thing] { hasIID: false, types: [person], predicates: [] } (start)
	edges:
		1: ($c <--[ROLEPLAYER]--* $_1) { roleTypes: [gift-delivery:recipient] }
		2: ($_1 *--[ROLEPLAYER]--> $b) { roleTypes: [gift-delivery:sender] }
		3: ($b <--[ROLEPLAYER]--* $_0) { roleTypes: [gift-delivery:recipient] }
		4: ($_0 *--[ROLEPLAYER]--> $a) { roleTypes: [gift-delivery:sender] }
}
         */

            proc.backwardRolePlayer(1, c, _1, set(Label.of("recipient", "gift-delivery")));
            proc.forwardRolePlayer(2, _1, b, set(Label.of("sender", "gift-delivery")));
            proc.backwardRolePlayer(3, b, _0, set(Label.of("recipient", "gift-delivery")));
            proc.forwardRolePlayer(4, _0, a, set(Label.of("sender", "gift-delivery")));

            List<Identifier.Variable.Name> filter = Arrays.asList(
                    a.id().asVariable().asName(),
                    b.id().asVariable().asName(),
                    c.id().asVariable().asName()
            );

            GraphProcedure procedure = proc.build();
            ResourceIterator<VertexMap> vertices = transaction.traversal().iterator(procedure, params, filter);
            assertTrue(vertices.hasNext());
            ResourceIterator<ConceptMap> answers = transaction.concepts().conceptMaps(vertices);
            assertTrue(answers.hasNext());
            ConceptMap answer;
            while (answers.hasNext()) {
                answer = answers.next();
                System.out.println(answer);
            }
        }
    }

}

