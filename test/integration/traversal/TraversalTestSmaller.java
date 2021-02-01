package grakn.core.test.integration.traversal;

import grakn.core.Grakn;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Arguments;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.type.AttributeType;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static grakn.core.common.parameters.Arguments.Transaction.Type.READ;
import static java.nio.charset.StandardCharsets.UTF_8;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class TraversalTestSmaller {

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
            GraqlDefine query = Graql.parseQuery(new String(Files.readAllBytes(Paths.get("test/integration/traversal/schema.gql")), UTF_8));
            transaction.query().define(query);
            transaction.commit();
        }
    }

    @AfterClass
    public static void teardown() {
        grakn.close();
    }

    @Test
    public void test_traversal_procedure() {
        try (RocksTransaction transaction = session.transaction(READ)) {
            GraphProcedure.Builder proc = GraphProcedure.builder(20);
            Traversal.Parameters params = new Traversal.Parameters();

            ProcedureVertex.Type _cgdb_cell_line_id = proc.labelledType("cgdb-cell-line-id", true);
            proc.setLabel(_cgdb_cell_line_id, "cgdb-cell-line-id");
            ProcedureVertex.Type _assay = proc.labelledType("assay");
            proc.setLabel(_assay, "assay");
            ProcedureVertex.Type _attribute = proc.labelledType("attribute");
            proc.setLabel(_attribute, "attribute");
            ProcedureVertex.Type _cell_line = proc.labelledType("cell-line");
            proc.setLabel(_cell_line, "cell-line");
            ProcedureVertex.Type _cgdb_assay_id = proc.labelledType("cgdb-assay-id");
            proc.setLabel(_cgdb_assay_id, "cgdb-assay-id");
            ProcedureVertex.Type _cgdb_compound_id = proc.labelledType("cgdb-compound-id");
            proc.setLabel(_cgdb_compound_id, "cgdb-compound-id");
            ProcedureVertex.Type _cgdb_document_id = proc.labelledType("cgdb-document-id");
            proc.setLabel(_cgdb_document_id, "cgdb-document-id");
            ProcedureVertex.Type _compound = proc.labelledType("compound");
            proc.setLabel(_compound, "compound");
            ProcedureVertex.Type _document = proc.labelledType("document");
            proc.setLabel(_document, "document");
            ProcedureVertex.Type _organism = proc.labelledType("organism");
            proc.setLabel(_organism, "organism");
//            ProcedureVertex.Type _protein = proc.labelledType("protein");
//            proc.setLabel(_protein, "protein");
            ProcedureVertex.Type _tax_id = proc.labelledType("tax-id");
            proc.setLabel(_tax_id, "tax-id");
            ProcedureVertex.Type _uniprot_id = proc.labelledType("uniprot-id");
            proc.setLabel(_uniprot_id, "uniprot-id");

            ProcedureVertex.Type assay_0 = proc.namedType("assay-0");
            ProcedureVertex.Type cell_line_3 = proc.namedType("cell-line-3");
            ProcedureVertex.Type compound_1 = proc.namedType("compound-1");
            ProcedureVertex.Type document_4 = proc.namedType("document-4");
            ProcedureVertex.Type organism_2 = proc.namedType("organism-2");
//            ProcedureVertex.Type protein_5 = proc.namedType("protein-5");

            ProcedureVertex.Type sys_0 = proc.namedType("sys-0");
            ProcedureVertex.Type sys_1 = proc.namedType("sys-1");
            ProcedureVertex.Type sys_2 = proc.namedType("sys-2");
            ProcedureVertex.Type sys_3 = proc.namedType("sys-3");
            ProcedureVertex.Type sys_4 = proc.namedType("sys-4");
//            ProcedureVertex.Type sys_5 = proc.namedType("sys-5");


//		1: ($_cgdb-cell-line-id <--[SUB]--* $/3) { isTransitive: true }
            proc.backwardSub(1, _cgdb_cell_line_id, sys_3, true);
//		2: ($/3 *--[SUB]--> $_attribute) { isTransitive: true }
            proc.forwardSub(2, sys_3, _attribute, true);
//		3: ($_attribute <--[SUB]--* $/1) { isTransitive: true }
            proc.backwardSub(3, _attribute, sys_1, true);
//		4: ($_attribute <--[SUB]--* $/4) { isTransitive: true }
            proc.backwardSub(4, _attribute, sys_4, true);
////		5: ($_attribute <--[SUB]--* $/5) { isTransitive: true }
//            proc.backwardSub(5, _attribute, sys_5, true);
//		6: ($_attribute <--[SUB]--* $/2) { isTransitive: true }
            proc.backwardSub(5, _attribute, sys_2, true);
//		7: ($_attribute <--[SUB]--* $/0) { isTransitive: true }
            proc.backwardSub(6, _attribute, sys_0, true);
//		8: ($/1 <--[OWNS]--* $compound-1) { isKey: false }
            proc.backwardOwns(7, sys_1, compound_1, false);
//		9: ($/0 *--[SUB]--> $_cgdb-assay-id) { isTransitive: true }
            proc.forwardSub(8, sys_0, _cgdb_assay_id, true);
//		10: ($compound-1 *--[SUB]--> $_compound) { isTransitive: true }
            proc.forwardSub(9, compound_1, _compound, true);
//		11: ($/1 *--[SUB]--> $_cgdb-compound-id) { isTransitive: true }
            proc.forwardSub(10, sys_1, _cgdb_compound_id, true);
////		12: ($/5 *--[SUB]--> $_uniprot-id) { isTransitive: true }
//            proc.forwardSub(12, sys_5, _uniprot_id, true);
//		13: ($/4 *--[SUB]--> $_cgdb-document-id) { isTransitive: true }
            proc.forwardSub(11, sys_4, _cgdb_document_id, true);
//		14: ($/2 *--[SUB]--> $_tax-id) { isTransitive: true }
            proc.forwardSub(12, sys_2, _tax_id, true);
////		15: ($/5 <--[OWNS]--* $protein-5) { isKey: false }
//            proc.backwardOwns(15, sys_5, protein_5, false);
////		16: ($protein-5 *--[SUB]--> $_protein) { isTransitive: true }
//            proc.forwardSub(16, protein_5, _protein, true);
//		17: ($/4 <--[OWNS]--* $document-4) { isKey: false }
            proc.backwardOwns(13, sys_4, document_4, false);
//		18: ($document-4 *--[SUB]--> $_document) { isTransitive: true }
            proc.forwardSub(14, document_4, _document, true);
//		19: ($/0 <--[OWNS]--* $assay-0) { isKey: false }
            proc.backwardOwns(15, sys_0, assay_0, false);
//		20: ($assay-0 *--[SUB]--> $_assay) { isTransitive: true }
            proc.forwardSub(16, assay_0, _assay, true);
//		21: ($/3 <--[OWNS]--* $cell-line-3) { isKey: false }
            proc.backwardOwns(17, sys_3, cell_line_3, false);
//		22: ($/2 <--[OWNS]--* $organism-2) { isKey: false }
            proc.backwardOwns(18, sys_2, organism_2, false);
//		23: ($cell-line-3 *--[SUB]--> $_cell-line) { isTransitive: true }
            proc.forwardSub(19, cell_line_3, _cell_line, true);
//		24: ($organism-2 *--[SUB]--> $_organism) { isTransitive: true }
            proc.forwardSub(20, organism_2, _organism, true);

            List<Identifier.Variable.Name> filter =  Arrays.asList(
                    sys_0.id().asVariable().asName(),
                    sys_1.id().asVariable().asName(),
                    sys_2.id().asVariable().asName(),
//                    sys_3.id().asVariable().asName(),
                    sys_4.id().asVariable().asName(),
//                    sys_5.id().asVariable().asName(),
                    assay_0.asType().id().asVariable().asName(),
//                    protein_5.asType().id().asVariable().asName(),
//                    cell_line_3.asType().id().asVariable().asName(),
                    document_4.asType().id().asVariable().asName(),
                    organism_2.asType().id().asVariable().asName(),
                    compound_1.asType().id().asVariable().asName()
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

