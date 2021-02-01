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

public class TraversalTest {

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
            GraqlDefine query = Graql.parseQuery("define\n" +
                                                         "\n" +
                                                         "word sub entity,\n" +
                                                         "    plays inheritance:subtype,\n" +
                                                         "    plays inheritance:supertype,\n" +
                                                         "    plays pair:prep,\n" +
                                                         "    plays pair:pobj,\n" +
                                                         "    owns name;\n" +
                                                         "\n" +
                                                         "f sub word;\n" +
                                                         "o sub word;\n" +
                                                         "\n" +
                                                         "inheritance sub relation,\n" +
                                                         "    relates supertype,\n" +
                                                         "    relates subtype;\n" +
                                                         "\n" +
                                                         "pair sub relation,\n" +
                                                         "    relates prep,\n" +
                                                         "    relates pobj,\n" +
                                                         "    owns typ,\n" +
                                                         "    owns name;\n" +
                                                         "\n" +
                                                         "name sub attribute, value string;\n" +
            "typ sub attribute, value string;");
            transaction.query().define(query);
            transaction.commit();
        }

        session.close();
        session = grakn.session(database, Arguments.Session.Type.DATA);
        try (Grakn.Transaction tx = session.transaction(WRITE)) {
            tx.query().insert(Graql.parseQuery("" +
                                                  "      insert\n" +
                                                  "\n" +
                                                  "      $f isa f, has name \"f\";\n" +
                                                  "      $o isa o, has name \"o\";\n" +
                                                  "\n" +
                                                  "      $aa isa word, has name \"aa\";\n" +
                                                  "      $bb isa word, has name \"bb\";\n" +
                                                  "      $cc isa word, has name \"cc\";\n" +
                                                  "\n" +
                                                  "      (supertype: $o, subtype: $aa) isa inheritance;\n" +
                                                  "      (supertype: $o, subtype: $bb) isa inheritance;\n" +
                                                  "      (supertype: $o, subtype: $cc) isa inheritance;\n" +
                                                  "\n" +
                                                  "      $pp isa word, has name \"pp\";\n" +
                                                  "      $qq isa word, has name \"qq\";\n" +
                                                  "      $rr isa word, has name \"rr\";\n" +
                                                  "      $rr2 isa word, has name \"rr\";\n" +
                                                  "\n" +
                                                  "      (supertype: $f, subtype: $pp) isa inheritance;\n" +
                                                  "      (supertype: $f, subtype: $qq) isa inheritance;\n" +
                                                  "      (supertype: $f, subtype: $rr) isa inheritance;\n" +
                                                  "      (supertype: $f, subtype: $rr2) isa inheritance;" +

//                                                       "    (prep: $aa, pobj: $aa) isa pair;" +
//                                                       "    (prep: $aa, pobj: $bb) isa pair;" +
//                                                       "    (prep: $aa, pobj: $cc) isa pair;" +
//                                                       "    (prep: $aa, pobj: $pp) isa pair;" +
//                                                       "    (prep: $aa, pobj: $qq) isa pair;" +
//                                                       "    (prep: $aa, pobj: $rr) isa pair;" +
//                                                       "    (prep: $aa, pobj: $rr2) isa pair;" +
//
//                                                       "    (prep: $bb, pobj: $aa) isa pair;" +
//                                                       "    (prep: $bb, pobj: $bb) isa pair;" +
//                                                       "    (prep: $bb, pobj: $cc) isa pair;" +
//                                                       "    (prep: $bb, pobj: $pp) isa pair;" +
//                                                       "    (prep: $bb, pobj: $qq) isa pair;" +
//                                                       "    (prep: $bb, pobj: $rr) isa pair;" +
//                                                       "    (prep: $bb, pobj: $rr2) isa pair;" +

//                                                       "    (prep: $cc, pobj: $aa) isa pair;" +
//                                                       "    (prep: $cc, pobj: $bb) isa pair;" +
//                                                       "    (prep: $cc, pobj: $cc) isa pair;" +
//                                                       "    (prep: $cc, pobj: $pp) isa pair;" +
//                                                       "    (prep: $cc, pobj: $qq) isa pair;" +
//                                                       "    (prep: $cc, pobj: $rr) isa pair;" +
//                                                       "    (prep: $cc, pobj: $rr2) isa pair;" +

//                                                       "    (prep: $pp, pobj: $aa) isa pair;" +
//                                                       "    (prep: $pp, pobj: $bb) isa pair;" +
//                                                       "    (prep: $pp, pobj: $cc) isa pair;" +
                                                       "    (prep: $pp, pobj: $pp) isa pair;" +
                                                       "    (prep: $pp, pobj: $qq) isa pair;" +
                                                       "    (prep: $pp, pobj: $rr) isa pair;" +
//                                                       "    (prep: $pp, pobj: $rr2) isa pair;" +

//                                                       "    (prep: $qq, pobj: $aa) isa pair;" +
//                                                       "    (prep: $qq, pobj: $bb) isa pair;" +
//                                                       "    (prep: $qq, pobj: $cc) isa pair;" +
                                                       "    (prep: $qq, pobj: $pp) isa pair;" +
                                                       "    (prep: $qq, pobj: $qq) isa pair;" +
                                                       "    (prep: $qq, pobj: $rr) isa pair;" +
//                                                       "    (prep: $qq, pobj: $rr2) isa pair;" +

//                                                       "    (prep: $rr, pobj: $aa) isa pair;" +
//                                                       "    (prep: $rr, pobj: $bb) isa pair;" +
//                                                       "    (prep: $rr, pobj: $cc) isa pair;" +
                                                       "    (prep: $rr, pobj: $pp) isa pair;" +
                                                       "    (prep: $rr, pobj: $qq) isa pair;" +
                                                       "    (prep: $rr, pobj: $rr) isa pair;" +
//                                                       "    (prep: $rr, pobj: $rr2) isa pair;" +

//                                                       "    (prep: $rr2, pobj: $aa) isa pair;" +
//                                                       "    (prep: $rr2, pobj: $bb) isa pair;" +
//                                                       "    (prep: $rr2, pobj: $cc) isa pair;" +
                                                       "    (prep: $rr2, pobj: $pp) isa pair;" +
                                                       "    (prep: $rr2, pobj: $qq) isa pair;" +
                                                       "    (prep: $rr2, pobj: $rr) isa pair;" +
//                                                       "    (prep: $rr2, pobj: $rr2) isa pair;" +
                                                   ""));
            tx.commit();
        }
    }

    @AfterClass
    public static void teardown() {
        grakn.close();
    }

    @Test
    public void test_traversal_procedure() {
        try (RocksTransaction transaction = session.transaction(READ)) {
            GraphProcedure.Builder proc = GraphProcedure.builder(6);
            Traversal.Parameters params = new Traversal.Parameters();

            /*
            	vertices:
		$_0 [thing] { hasIID: false, types: [inheritance], predicates: [] }
		$_1 [thing] { hasIID: false, types: [inheritance], predicates: [] }
		$f [thing] { hasIID: false, types: [f], predicates: [] } (end)
		$p [thing] { hasIID: true, types: [pair], predicates: [] } (start)
		$pobj [thing] { hasIID: false, types: [o, word, f], predicates: [] }
		$prep [thing] { hasIID: false, types: [o, word, f], predicates: [] }
        */

            ProcedureVertex.Thing p = proc.namedThing("p", true);
            p.props().types(set(Label.of("pair")));
            ProcedureVertex.Thing pobj = proc.namedThing("pobj");
            pobj.props().types(set(Label.of("o"), Label.of("word"), Label.of("f")));
            ProcedureVertex.Thing prep = proc.namedThing("prep");
            prep.props().types(set(Label.of("o"), Label.of("word"), Label.of("f")));
            ProcedureVertex.Thing f = proc.namedThing("f");
            f.props().types(set(Label.of("f")));
            ProcedureVertex.Thing _0 = proc.anonymousThing(0);
            _0.props().types(set(Label.of("inheritance")));
            ProcedureVertex.Thing _1 = proc.anonymousThing(1);
            _1.props().types(set(Label.of("inheritance")));

            /*
            edges:
            1: ($p *--[ROLEPLAYER]--> $pobj) { roleTypes: [pair:pobj] }
            2: ($p *--[ROLEPLAYER]--> $prep) { roleTypes: [pair:prep] }
            */
            proc.forwardRolePlayer(1, p, pobj, set(Label.of("pobj", "pair")));
            proc.forwardRolePlayer(2, p, prep, set(Label.of("prep", "pair")));
            /*
            3: ($prep <--[ROLEPLAYER]--* $_0) { roleTypes: [inheritance:subtype] }
            4: ($pobj <--[ROLEPLAYER]--* $_1) { roleTypes: [inheritance:subtype] }
            5: ($_0 *--[ROLEPLAYER]--> $f) { roleTypes: [inheritance:supertype] }
            6: ($_1 *--[ROLEPLAYER]--> $f) { roleTypes: [inheritance:supertype] }
             */
            proc.backwardRolePlayer(3, prep, _0, set(Label.of("subtype", "inheritance")));
            proc.backwardRolePlayer(4, pobj, _1, set(Label.of("subtype", "inheritance")));
            proc.forwardRolePlayer(5, _0, f, set(Label.of("supertype", "inheritance")));
            proc.forwardRolePlayer(6, _1, f, set(Label.of("supertype", "inheritance")));

            List<Identifier.Variable.Name> filter =  Arrays.asList(
                    f.id().asVariable().asName(),
                    p.id().asVariable().asName(),
                    pobj.id().asVariable().asName(),
                    prep.id().asVariable().asName()
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

