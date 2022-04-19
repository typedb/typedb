/*
 * Copyright (C) 2021 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.traversal;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreSession;
import com.vaticle.typedb.core.database.CoreTransaction;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.predicate.Predicate;
import com.vaticle.typedb.core.traversal.predicate.PredicateArgument;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;
import com.vaticle.typedb.core.traversal.procedure.ProcedureVertex;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.common.TypeQLToken;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.parameters.Arguments.Transaction.Type.READ;
import static com.vaticle.typedb.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static org.junit.Assert.assertEquals;

public class TraversalTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("query-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageIndexCacheSize(MB).storageDataCacheSize(MB);
    private static String database = "query-test";

    private static CoreDatabaseManager databaseMgr;
    private static CoreSession session;

    @BeforeClass
    public static void setup() throws IOException {
        Util.resetDirectory(dataDir);
        databaseMgr = CoreDatabaseManager.open(options);
        databaseMgr.create(database);
        session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA);

    }

    @AfterClass
    public static void teardown() {
        databaseMgr.close();
    }

    @Test
    public void mixed_variable_and_edge_role_players() {
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            TypeQLDefine query = TypeQL.parseQuery(
                    "define " +
                            "person sub entity, " +
                            "  plays employment:employee; " +
                            "company sub entity, " +
                            "  plays employment:employer;" +
                            "employment sub relation, " +
                            "  relates employer, " +
                            "  relates employee; "
            );
            transaction.query().define(query);
            transaction.commit();
        }
        session.close();

        session = databaseMgr.session(database, Arguments.Session.Type.DATA);
        // note: must insert in separate Tx's so that the relations are retrieved in a specific order later
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            transaction.query().insert(TypeQL.parseQuery(
                    "insert " +
                            "$x isa company;" +
                            "$y isa person;" +
                            "(employer: $x, employee: $y) isa employment;").asInsert()
            );
            transaction.commit();
        }
        try (CoreTransaction transaction = session.transaction(READ)) {
            /*
            match (employer: $e, $role: $x) isa employment;
            */
            /*
            Edges:
            	0: $_0 [thing] { hasIID: false, types: [employment], predicates: [] } (start)
            	1: $_0:$role:$x:1 [thing] { hasIID: false, types: [employment:employer, employment:employee], predicates: [] }
            			1: ($_0 *--[RELATING]--> $_0:$role:$x:1)
            	2: $e [thing] { hasIID: false, types: [company], predicates: [] } (end)
            			2: ($_0 *--[ROLEPLAYER]--> $e) { roleTypes: [employment:employer] }
            	3: $x [thing] { hasIID: false, types: [person, company], predicates: [] } (end)
            			3: ($_0:$role:$x:1 <--[PLAYING]--* $x)
            	4: $role [type] { labels: [employment:employer, relation:role, employment:employee], abstract: false, value: [], regex: null } (end)
            			4: ($_0:$role:$x:1 *--[ISA]--> $role) { isTransitive: true }
            */
            GraphProcedure.Builder proc = GraphProcedure.builder(5);
            ProcedureVertex.Thing _0 = proc.anonymousThing(0, 0, true);
            _0.props().types(set(Label.of("employment")));

            ProcedureVertex.Thing e = proc.namedThing(2, "e");
            e.props().types(set(Label.of("company")));

            ProcedureVertex.Type role = proc.namedType(4, "role");
            role.props().labels(set(Label.of("employer", "employment"), Label.of("employee", "employment"), Label.of("role", "relation")));

            ProcedureVertex.Thing x = proc.namedThing(3, "x");
            x.props().types(set(Label.of("person"), Label.of("company")));

            ProcedureVertex.Thing role_inst = proc.scopedThing(1, _0, role, x, 0);
            role_inst.props().types(set(Label.of("employer", "employment"), Label.of("employee", "employment")));

            proc.forwardRelating(_0, role_inst);
            proc.backwardPlaying(role_inst, x);
            proc.forwardIsa(role_inst, role, true);
            proc.forwardRolePlayer(_0, e, set(Label.of("employer", "employment")));

            Traversal.Parameters params = new Traversal.Parameters();

            Set<Identifier.Variable.Retrievable> filter = set(
                    _0.id().asVariable().asRetrievable(),
                    e.id().asVariable().asRetrievable(),
                    x.id().asVariable().asRetrievable(),
                    role.id().asVariable().asRetrievable()
            );

            GraphProcedure procedure = proc.build();
            FunctionalIterator<VertexMap> vertices = procedure.iterator(transaction.traversal().graph(), params, filter);
            assertEquals(2, vertices.count());
        }
    }


    @Test
    public void test() {
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            TypeQLDefine query = TypeQL.parseQuery(
                    "define " +
                            "person sub entity," +
                            "  owns name," +
                            "  owns age," +
                            "  plays friendship:friend," +
                            "  plays employment:employee;" +
                            "company sub entity," +
                            "  owns name," +
                            "  plays employment:employer;" +
                            "place sub entity," +
                            "  owns name," +
                            "  plays location-hierarchy:subordinate," +
                            "  plays location-hierarchy:superior;" +
                            "friendship sub relation," +
                            "  relates friend;" +
                            "employment sub relation," +
                            "  relates employee," +
                            "  relates employer;" +
                            "location-hierarchy sub relation," +
                            "  relates subordinate," +
                            "  relates superior;" +
                            "name sub attribute, value string;" +
                            "age sub attribute, value long;"
            );
            transaction.query().define(query);
            transaction.commit();
        }
        session.close();

        session = databaseMgr.session(database, Arguments.Session.Type.DATA);
        try (CoreTransaction transaction = session.transaction(READ)) {
            /*
            Edges:
	        0: $_5 [type] { labels: [], abstract: false, value: [], regex: null } (start)
	        1: $_6 [type] { labels: [link:to], abstract: false, value: [], regex: null } (end)
	        		1: ($_5 *--[SUB]--> $_6) { isTransitive: true }
	        2: $_1 [type] { labels: [link], abstract: false, value: [], regex: null }
	        		2: ($_5 <--[RELATES]--* $_1)
	        3: $y [type] { labels: [traversable, vertex, node], abstract: false, value: [], regex: null } (end)
	        		3: ($_5 <--[PLAYS]--* $y)
	        4: $_3 [type] { labels: [], abstract: false, value: [], regex: null }
	        		4: ($_1 *--[RELATES]--> $_3)
	        5: $x [type] { labels: [traversable, vertex, node], abstract: false, value: [], regex: null } (end)
	        		5: ($_3 <--[PLAYS]--* $x)
	        6: $_4 [type] { labels: [link:from], abstract: false, value: [], regex: null } (end)
	        		6: ($_3 *--[SUB]--> $_4) { isTransitive: true }
	        7: $_2 [type] { labels: [link], abstract: false, value: [], regex: null } (end)
	        		7: ($_1 *--[SUB]--> $_2) { isTransitive: true }
            */

            // TODO complete test



//            GraphProcedure.Builder proc = GraphProcedure.builder(5);
//            ProcedureVertex.Thing _0 = proc.anonymousThing(0, 0, true);
//            _0.props().types(set(Label.of("employment")));
//
//            ProcedureVertex.Thing e = proc.namedThing(2, "e");
//            e.props().types(set(Label.of("company")));
//
//            ProcedureVertex.Type role = proc.namedType(4, "role");
//            role.props().labels(set(Label.of("employer", "employment"), Label.of("employee", "employment"), Label.of("role", "relation")));
//
//            ProcedureVertex.Thing x = proc.namedThing(3, "x");
//            x.props().types(set(Label.of("person"), Label.of("company")));
//
//            ProcedureVertex.Thing role_inst = proc.scopedThing(1, _0, role, x, 0);
//            role_inst.props().types(set(Label.of("employer", "employment"), Label.of("employee", "employment")));
//
//            proc.forwardRelating(_0, role_inst);
//            proc.backwardPlaying(role_inst, x);
//            proc.forwardIsa(role_inst, role, true);
//            proc.forwardRolePlayer(_0, e, set(Label.of("employer", "employment")));
//
//            Traversal.Parameters params = new Traversal.Parameters();
//
//            Set<Identifier.Variable.Retrievable> filter = set(
//                    _0.id().asVariable().asRetrievable(),
//                    e.id().asVariable().asRetrievable(),
//                    x.id().asVariable().asRetrievable(),
//                    role.id().asVariable().asRetrievable()
//            );
//
//            GraphProcedure procedure = proc.build();
//            FunctionalIterator<VertexMap> vertices = procedure.iterator(transaction.traversal().graph(), params, filter);
//            assertEquals(2, vertices.count());
        }
    }

    /**
     * This test is interesting because it invalidates a traversal optimisation we implemented called `SeekStack`
     * where we backtrack all the way back to the cause of a branch/closure failure, skipping all the query vertices
     * that are planned in the middle.
     *
     * However this test shows that it is possible to successfully do a closure, then do perform a branch failure
     * that because of the Seek optimisation jumps too far back and fails to generate other closure candidates, and
     * misses an answer.
     **/
    @Test
    public void backtrack_seeks_do_not_skip_answers() {
        try (TypeDB.Transaction transaction = session.transaction(WRITE)) {
            TypeQLDefine query = TypeQL.parseQuery(
                    "define " +
                            "person sub entity, " +
                            "  plays friendship:friend, " +
                            "  owns name;" +
                            "dog sub entity," +
                            "  plays friendship:friend; " +
                            "friendship sub relation, " +
                            "  relates friend; " +
                            "name sub attribute, value string; "
            );
            transaction.query().define(query);
            transaction.commit();
        }
        session.close();

        session = databaseMgr.session(database, Arguments.Session.Type.DATA);
        // note: must insert in separate Tx's so that the relations are retrieved in a specific order later
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            transaction.query().insert(TypeQL.parseQuery(
                    "insert $x isa person; (friend: $x) isa friendship;").asInsert()
            );
            transaction.commit();
        }
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            transaction.query().insert(TypeQL.parseQuery("insert" +
                    "$y isa dog; (friend: $y) isa friendship;").asInsert());
            transaction.commit();
        }
        try (CoreTransaction transaction = session.transaction(READ)) {
            /*
            match
            $rel ($role: $friend);
            $friend isa dog;
            $rel isa $rel-type;
            $rel-type type friendship, relates $role;
            $role type friendship:friend;
            */
            /*
                    1: ($rel-type *--[RELATES]--> $role)
                    2: ($rel-type <--[ISA]--* $rel) { isTransitive: true }
                    3: ($role <--[ISA]--* $rel:$role:$friend:1) { isTransitive: true }
                    4: ($rel *--[RELATING]--> $rel:$role:$friend:1)
                    5: ($rel:$role:$friend:1 <--[PLAYING]--* $friend)
            */
            GraphProcedure.Builder proc = GraphProcedure.builder(5);
            ProcedureVertex.Type rel_type = proc.namedType(0, "rel-type", true);
            rel_type.props().labels(set(Label.of("friendship")));

            ProcedureVertex.Type role_type = proc.namedType(1, "role-type");
            role_type.props().labels(set(Label.of("friend", "friendship")));

            ProcedureVertex.Thing rel = proc.namedThing(2, "rel");
            rel.props().types(set(Label.of("friendship")));

            ProcedureVertex.Thing friend = proc.namedThing(4, "friend");
            friend.props().types(set(Label.of("dog")));

            ProcedureVertex.Thing role = proc.scopedThing(3, rel, role_type, friend, 0);
            role.props().types(set(Label.of("friend", "friendship")));

            proc.forwardRelates(rel_type, role_type);
            proc.backwardIsa(rel_type, rel, true);
            proc.backwardIsa(role_type, role, true);
            proc.forwardRelating(rel, role);
            proc.backwardPlaying(role, friend);

            Traversal.Parameters params = new Traversal.Parameters();

            Set<Identifier.Variable.Retrievable> filter = set(
                    rel_type.id().asVariable().asRetrievable(),
                    role_type.id().asVariable().asRetrievable(),
                    rel.id().asVariable().asRetrievable(),
                    friend.id().asVariable().asRetrievable()
            );

            GraphProcedure procedure = proc.build();
            FunctionalIterator<VertexMap> vertices = procedure.iterator(transaction.traversal().graph(), params, filter);
            assertEquals(1, vertices.count());
        }
    }

    @Test
    public void test_closure_backtrack_clears_scopes() {
        try (TypeDB.Transaction transaction = session.transaction(WRITE)) {
            TypeQLDefine query = TypeQL.parseQuery(
                    "define " +
                            "person sub entity, " +
                            "  plays friendship:friend, " +
                            "  owns name," +
                            "  owns lastname;" +
                            "dog sub entity," +
                            "  plays friendship:friend; " +
                            "friendship sub relation, " +
                            "  relates friend," +
                            "  owns ref; " +
                            "name sub attribute, value string; " +
                            "lastname sub attribute, value string;" +
                            "ref sub attribute, value long;"
            );
            transaction.query().define(query);
            transaction.commit();
        }
        session.close();

        session = databaseMgr.session(database, Arguments.Session.Type.DATA);
        try (TypeDB.Transaction transaction = session.transaction(WRITE)) {
            TypeQLInsert query = TypeQL.parseQuery("insert " +
                    "$x isa person," +
                    "  has lastname \"Smith\"," +
                    "  has name \"Alex\";" +
                    "$y isa person," +
                    "  has lastname \"Smith\"," +
                    "  has name \"John\";" +
                    "$r (friend: $x, friend: $y) isa friendship, has ref 1;" +
                    "$r1 (friend: $x, friend: $y) isa friendship, has ref 2;" +
                    "$reflexive (friend: $x, friend: $x) isa friendship, has ref 3;").asInsert();

            transaction.query().insert(query);
            transaction.commit();
        }

        try (CoreTransaction transaction = session.transaction(READ)) {
            GraphProcedure.Builder proc = GraphProcedure.builder(9);
            /*
            vertices:
            $_0 [thing] { hasIID: false, types: [name], predicates: [= <STRING>] } (end) // Alex
            $_1 [thing] { hasIID: false, types: [name], predicates: [= <STRING>] } (end) // John
            $f1 [thing] { hasIID: false, types: [friendship], predicates: [] }
            $n [thing] { hasIID: false, types: [lastname], predicates: [] } (start)
            $r1 [thing] { hasIID: false, types: [ref], predicates: [= <LONG>] } (end) // 3
            $r2 [thing] { hasIID: false, types: [ref], predicates: [= <LONG>] } (end) // 1
            $refl [thing] { hasIID: false, types: [friendship], predicates: [] }
            $x [thing] { hasIID: false, types: [person], predicates: [] }
            $y [thing] { hasIID: false, types: [person], predicates: [] }

            edges:
            1: ($n <--[HAS]--* $x)
            2: ($n <--[HAS]--* $y)
            3: ($x *--[HAS]--> $_0)
            4: ($x <--[ROLEPLAYER]--* $refl) { roleTypes: [friendship:friend] }
            5: ($x <--[ROLEPLAYER]--* $refl) { roleTypes: [friendship:friend] }
            6: ($x <--[ROLEPLAYER]--* $f1) { roleTypes: [friendship:friend] }
            7: ($y *--[HAS]--> $_1)
            8: ($y <--[ROLEPLAYER]--* $f1) { roleTypes: [friendship:friend] }
            9: ($refl *--[HAS]--> $r1)
            10: ($f1 *--[HAS]--> $r2)
             */

            ProcedureVertex.Thing n = proc.namedThing(0, "n", true);
            n.props().types(set(Label.of("lastname")));

            ProcedureVertex.Thing x = proc.namedThing(1, "x");
            x.props().types(set(Label.of("person")));

            ProcedureVertex.Thing y = proc.namedThing(2, "y");
            y.props().types(set(Label.of("person")));

            ProcedureVertex.Thing _0 = proc.anonymousThing(3, 0);
            _0.props().predicate(Predicate.Value.String.of(TypeQLToken.Predicate.Equality.EQ));
            _0.props().types(set(Label.of("name")));

            ProcedureVertex.Thing refl = proc.namedThing(4, "refl");
            refl.props().types(set(Label.of("friendship")));

            ProcedureVertex.Thing f1 = proc.namedThing(5, "f1");
            f1.props().types(set(Label.of("friendship")));

            ProcedureVertex.Thing _1 = proc.anonymousThing(6, 1);
            _1.props().predicate(Predicate.Value.String.of(TypeQLToken.Predicate.Equality.EQ));
            _1.props().types(set(Label.of("name")));

            ProcedureVertex.Thing r1 = proc.namedThing(7, "r1");
            r1.props().predicate(Predicate.Value.Numerical.of(TypeQLToken.Predicate.Equality.EQ, PredicateArgument.Value.LONG));
            r1.props().types(set(Label.of("ref")));

            ProcedureVertex.Thing r2 = proc.namedThing(8, "r2");
            r2.props().predicate(Predicate.Value.Numerical.of(TypeQLToken.Predicate.Equality.EQ, PredicateArgument.Value.LONG));
            r2.props().types(set(Label.of("ref")));


            GraphTraversal.Thing.Parameters params = new GraphTraversal.Thing.Parameters();
            params.pushValue(_0.id().asVariable(),
                    Predicate.Value.String.of(TypeQLToken.Predicate.Equality.EQ),
                    new GraphTraversal.Thing.Parameters.Value("Alex"));
            params.pushValue(_1.id().asVariable(),
                    Predicate.Value.String.of(TypeQLToken.Predicate.Equality.EQ),
                    new GraphTraversal.Thing.Parameters.Value("John"));
            params.pushValue(r1.id().asVariable(),
                    Predicate.Value.Numerical.of(TypeQLToken.Predicate.Equality.EQ, PredicateArgument.Value.LONG),
                    new GraphTraversal.Thing.Parameters.Value(3L));
            params.pushValue(r2.id().asVariable(),
                    Predicate.Value.Numerical.of(TypeQLToken.Predicate.Equality.EQ, PredicateArgument.Value.LONG),
                    new GraphTraversal.Thing.Parameters.Value(1L));

            proc.backwardHas(n, x);
            proc.backwardHas(n, y);
            proc.forwardHas(x, _0);
            proc.backwardRolePlayer(x, refl, set(Label.of("friend", "friendship")));
            proc.backwardRolePlayer(x, refl, set(Label.of("friend", "friendship")));
            proc.backwardRolePlayer(x, f1, set(Label.of("friend", "friendship")));
            proc.forwardHas(y, _1);
            proc.backwardRolePlayer(y, f1, set(Label.of("friend", "friendship")));
            proc.forwardHas(refl, r1);
            proc.forwardHas(f1, r2);

            Set<Identifier.Variable.Retrievable> filter = set(
                    n.id().asVariable().asRetrievable(),
                    x.id().asVariable().asRetrievable(),
                    y.id().asVariable().asRetrievable(),
                    refl.id().asVariable().asRetrievable(),
                    f1.id().asVariable().asRetrievable(),
                    r1.id().asVariable().asRetrievable(),
                    r1.id().asVariable().asRetrievable(),
                    _0.id().asVariable().asRetrievable(),
                    _1.id().asVariable().asRetrievable()
            );

            GraphProcedure procedure = proc.build();
            FunctionalIterator<VertexMap> vertices = procedure.iterator(transaction.traversal().graph(), params, filter);
            vertices.next();
        }
        session.close();
    }

}

