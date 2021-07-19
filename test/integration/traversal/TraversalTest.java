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
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
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
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.parameters.Arguments.Transaction.Type.READ;

public class TraversalTest {

    private static final Path dataDir = Paths.get("/Users/joshua/Documents/grakn-debug/jon-thompson/typedb-all-mac/server/data");// Paths.get(System.getProperty("user.dir")).resolve("query-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).logsDir(logDir);
    private static String database = "tenancy";//"query-test";

    private static RocksTypeDB typedb;
    private static RocksSession session;

    @BeforeClass
    public static void setup() throws IOException {
//        Util.resetDirectory(dataDir);
        typedb = RocksTypeDB.open(options);
//        typedb.databases().create(database);
//        session = typedb.session(database, Arguments.Session.Type.SCHEMA);
//        try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
//            TypeQLDefine query = TypeQL.parseQuery("define " +
//                                                           "person sub entity, " +
//                                                           "  plays friendship:friend, " +
//                                                           "  owns name @key; " +
//                                                           "friendship sub relation, " +
//                                                           "  relates friend, " +
//                                                           "  owns ref @key; " +
//                                                           "name sub attribute, value string; " +
//                                                           "ref sub attribute, value long; "
//            );
//            transaction.query().define(query);
//            transaction.commit();
//        }
//        session.close();
    }

    @AfterClass
    public static void teardown() {
        typedb.close();
    }

    @Test
    public void test() {
        session = typedb.session(database, Arguments.Session.Type.DATA);
        /*
        Parameters: {
    	    values: {pair($_1, contains <STRING>)=[string: Bentsen], pair($_0, contains <STRING>)=[string: Mufti]}
        }
        vertices:
		$_0 [thing] { hasIID: false, types: [name], predicates: [contains <STRING>] } (end)
		$_1 [thing] { hasIID: false, types: [name], predicates: [contains <STRING>] } (end)
		$neighbour [thing] { hasIID: false, types: [person, organisation, corporation], predicates: [] }
		$neighbour_role [type] { labels: [tenancy:landlord, tenancy:tenant], abstract: false, value: [], regex: null }
		$node [thing] { hasIID: false, types: [person], predicates: [] }
		$node_role [type] { labels: [tenancy:landlord, tenancy:tenant], abstract: false, value: [], regex: null }
		$relation [thing] { hasIID: false, types: [tenancy, subtenancy], predicates: [] }
		$relation:$neighbour_role:$neighbour:1 [thing] { hasIID: false, types: [], predicates: [] }
		$relation:$node_role:$node:1 [thing] { hasIID: false, types: [], predicates: [] }
		$reltype [type] { labels: [tenancy], abstract: false, value: [], regex: null } (start)
	    edges:
		1: ($reltype *--[RELATES]--> $neighbour_role)
		2: ($reltype *--[RELATES]--> $node_role)
		3: ($reltype <--[ISA]--* $relation) { isTransitive: true }
		4: ($neighbour_role <--[ISA]--* $relation:$neighbour_role:$neighbour:1) { isTransitive: true }
		5: ($node_role <--[ISA]--* $relation:$node_role:$node:1) { isTransitive: true }
		6: ($relation *--[RELATING]--> $relation:$neighbour_role:$neighbour:1)
		7: ($relation *--[RELATING]--> $relation:$node_role:$node:1)
		8: ($relation:$neighbour_role:$neighbour:1 <--[PLAYING]--* $neighbour)
		9: ($relation:$node_role:$node:1 <--[PLAYING]--* $node)
		10: ($neighbour *--[HAS]--> $_0)
		11: ($node *--[HAS]--> $_1)
         */
        try (RocksTransaction transaction = session.transaction(READ)) {
            GraphProcedure.Builder proc = GraphProcedure.builder(11);
            ProcedureVertex.Thing _0 = proc.anonymousThing(0);
            _0.props().types(set(Label.of("name")));
            _0.props().predicate(Predicate.Value.String.of(TypeQLToken.Predicate.SubString.CONTAINS));

            ProcedureVertex.Thing _1 = proc.anonymousThing(1);
            _1.props().types(set(Label.of("name")));
            _1.props().predicate(Predicate.Value.String.of(TypeQLToken.Predicate.SubString.CONTAINS));

            ProcedureVertex.Thing neighbour = proc.namedThing("neighbour");
            neighbour.props().types(set(Label.of("person")));//, Label.of("organsation"), Label.of("corporation")));

            ProcedureVertex.Type neighbourRole = proc.namedType("neighbour_role");
            neighbourRole.props().labels(set(Label.of("landlord", "tenancy")));//, Label.of("tenant", "tenancy") ));

            ProcedureVertex.Thing node = proc.namedThing("node");
            node.props().types(set(Label.of("person")));

            ProcedureVertex.Type nodeRole = proc.namedType("node_role");
            nodeRole.props().labels(set(Label.of("tenant", "tenancy")));//, Label.of("landlord", "tenancy")));

            ProcedureVertex.Thing relation = proc.namedThing("relation");
            relation.props().types(set(Label.of("tenancy")));//, Label.of("subtenancy")));

            ProcedureVertex.Thing neighborRoleInst = proc.scopedThing(relation, neighbourRole, neighbour, 1);

            ProcedureVertex.Thing nodeRoleInst = proc.scopedThing(relation, nodeRole, node, 1);

            ProcedureVertex.Type reltype = proc.namedType("reltype", true);
            reltype.props().labels(set(Label.of("tenancy")));

            proc.forwardRelates(1, reltype, neighbourRole);
            proc.forwardRelates(2, reltype, nodeRole);
            proc.backwardIsa(3, reltype, relation, true);
            proc.backwardIsa(4, neighbourRole, neighborRoleInst, true);
            proc.backwardIsa(5, nodeRole, nodeRoleInst, true);
            proc.forwardRelating(6, relation, neighborRoleInst);
            proc.forwardRelating(7, relation, nodeRoleInst);
            proc.backwardPlaying(8, neighborRoleInst, neighbour);
            proc.backwardPlaying(9, nodeRoleInst, node);
            proc.forwardHas(10, neighbour, _0);
            proc.forwardHas(11, node, _1);

            Traversal.Parameters params = new Traversal.Parameters();
            params.pushValue(_0.id().asVariable(), Predicate.Value.String.of(TypeQLToken.Predicate.SubString.CONTAINS),
                    new Traversal.Parameters.Value("Mufti"));
            params.pushValue(_1.id().asVariable(), Predicate.Value.String.of(TypeQLToken.Predicate.SubString.CONTAINS),
                    new Traversal.Parameters.Value("Bentsen"));

            Set<Identifier.Variable.Retrievable> filter = set(
                    node.id().asVariable().asRetrievable(),
                    nodeRole.id().asVariable().asRetrievable(),
                    relation.id().asVariable().asRetrievable(),
                    neighbour.id().asVariable().asRetrievable(),
                    neighbourRole.id().asVariable().asRetrievable()
            );

            GraphProcedure procedure = proc.build();
            FunctionalIterator<VertexMap> vertices = transaction.traversal().iterator(procedure, params, filter);
            vertices.next();
        }
    }

    @Test
    public void test_closure_backtrack_clears_scopes() {
        session = typedb.session(database, Arguments.Session.Type.SCHEMA);
        try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
            TypeQLDefine query = TypeQL.parseQuery("define " +
                    "lastname sub attribute, value string; " +
                    "person sub entity, owns lastname; "

            );
            transaction.query().define(query);
            transaction.commit();
        }
        session.close();

        session = typedb.session(database, Arguments.Session.Type.DATA);
        try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
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

        try (RocksTransaction transaction = session.transaction(READ)) {
            GraphProcedure.Builder proc = GraphProcedure.builder(10);
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

             */

            ProcedureVertex.Thing _0 = proc.anonymousThing(0);
            _0.props().predicate(Predicate.Value.String.of(TypeQLToken.Predicate.Equality.EQ));

            ProcedureVertex.Thing _1 = proc.anonymousThing(1);
            _1.props().predicate(Predicate.Value.String.of(TypeQLToken.Predicate.Equality.EQ));

            ProcedureVertex.Thing f1 = proc.namedThing("f1");
            f1.props().types(set(Label.of("friendship")));

            ProcedureVertex.Thing refl = proc.namedThing("refl");
            refl.props().types(set(Label.of("friendship")));

            ProcedureVertex.Thing n = proc.namedThing("n", true);
            n.props().types(set(Label.of("lastname")));

            ProcedureVertex.Thing r1 = proc.namedThing("r1");
            r1.props().predicate(Predicate.Value.Numerical.of(TypeQLToken.Predicate.Equality.EQ, PredicateArgument.Value.LONG));

            ProcedureVertex.Thing r2 = proc.namedThing("r2");
            r2.props().predicate(Predicate.Value.Numerical.of(TypeQLToken.Predicate.Equality.EQ, PredicateArgument.Value.LONG));

            ProcedureVertex.Thing x = proc.namedThing("x");
            x.props().types(set(Label.of("person")));

            ProcedureVertex.Thing y = proc.namedThing("y");
            y.props().types(set(Label.of("person")));

            GraphTraversal.Parameters params = new GraphTraversal.Parameters();
            params.pushValue(_0.id().asVariable(),
                    Predicate.Value.String.of(TypeQLToken.Predicate.Equality.EQ),
                    new GraphTraversal.Parameters.Value("Alex"));
            params.pushValue(_1.id().asVariable(),
                    Predicate.Value.String.of(TypeQLToken.Predicate.Equality.EQ),
                    new GraphTraversal.Parameters.Value("John"));
            params.pushValue(r1.id().asVariable(),
                    Predicate.Value.Numerical.of(TypeQLToken.Predicate.Equality.EQ, PredicateArgument.Value.LONG),
                    new GraphTraversal.Parameters.Value(3L));
            params.pushValue(r2.id().asVariable(),
                    Predicate.Value.Numerical.of(TypeQLToken.Predicate.Equality.EQ, PredicateArgument.Value.LONG),
                    new GraphTraversal.Parameters.Value(1L));

            /*
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
            proc.backwardHas(1, n, x);
            proc.backwardHas(2, n, y);
            proc.forwardHas(3, x, _0);
            proc.backwardRolePlayer(4, x, refl, set(Label.of("friend", "friendship")));
            proc.backwardRolePlayer(5, x, refl, set(Label.of("friend", "friendship")));
            proc.backwardRolePlayer(6, x, f1, set(Label.of("friend", "friendship")));
            proc.forwardHas(7, y, _1);
            proc.backwardRolePlayer(8, y, f1, set(Label.of("friend", "friendship")));
            proc.forwardHas(9, refl, r1);
            proc.forwardHas(10, f1, r2);

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
            FunctionalIterator<VertexMap> vertices = transaction.traversal().iterator(procedure, params, filter);
            vertices.next();
        }
        session.close();
    }

}

