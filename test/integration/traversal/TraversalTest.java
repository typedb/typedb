/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.traversal;

import grakn.core.Grakn;
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Label;
import grakn.core.common.parameters.Options;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.predicate.Predicate;
import grakn.core.traversal.predicate.PredicateArgument;
import grakn.core.traversal.procedure.GraphProcedure;
import grakn.core.traversal.procedure.ProcedureVertex;
import graql.lang.Graql;
import graql.lang.common.GraqlToken;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlInsert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.parameters.Arguments.Transaction.Type.READ;

public class TraversalTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("query-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).logsDir(logDir);
    private static String database = "query-test";

    private static RocksGrakn grakn;
    private static RocksSession session;

    @BeforeClass
    public static void setup() throws IOException {
        Util.resetDirectory(dataDir);
        grakn = RocksGrakn.open(options);
        grakn.databases().create(database);
        session = grakn.session(database, Arguments.Session.Type.SCHEMA);
        try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
            GraqlDefine query = Graql.parseQuery("define " +
                                                         "person sub entity, " +
                                                         "  plays friendship:friend, " +
                                                         "  owns name @key; " +
                                                         "friendship sub relation, " +
                                                         "  relates friend, " +
                                                         "  owns ref @key; " +
                                                         "name sub attribute, value string; " +
                                                         "ref sub attribute, value long; "
            );
            transaction.query().define(query);
            transaction.commit();
        }
        session.close();
    }

    @AfterClass
    public static void teardown() {
        grakn.close();
    }

    @Test
    public void test_closure_backtrack_clears_scopes() {
        session = grakn.session(database, Arguments.Session.Type.SCHEMA);
        try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
            GraqlDefine query = Graql.parseQuery("define " +
                                                         "lastname sub attribute, value string; " +
                                                         "person sub entity, owns lastname; "

            );
            transaction.query().define(query);
            transaction.commit();
        }
        session.close();

        session = grakn.session(database, Arguments.Session.Type.DATA);
        try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
            GraqlInsert query = Graql.parseQuery("insert " +
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
            _0.props().predicate(Predicate.Value.String.of(GraqlToken.Predicate.Equality.EQ));

            ProcedureVertex.Thing _1 = proc.anonymousThing(1);
            _1.props().predicate(Predicate.Value.String.of(GraqlToken.Predicate.Equality.EQ));

            ProcedureVertex.Thing f1 = proc.namedThing("f1");
            f1.props().types(set(Label.of("friendship")));

            ProcedureVertex.Thing refl = proc.namedThing("refl");
            refl.props().types(set(Label.of("friendship")));

            ProcedureVertex.Thing n = proc.namedThing("n", true);
            n.props().types(set(Label.of("lastname")));

            ProcedureVertex.Thing r1 = proc.namedThing("r1");
            r1.props().predicate(Predicate.Value.Numerical.of(GraqlToken.Predicate.Equality.EQ, PredicateArgument.Value.LONG));

            ProcedureVertex.Thing r2 = proc.namedThing("r2");
            r2.props().predicate(Predicate.Value.Numerical.of(GraqlToken.Predicate.Equality.EQ, PredicateArgument.Value.LONG));

            ProcedureVertex.Thing x = proc.namedThing("x");
            x.props().types(set(Label.of("person")));

            ProcedureVertex.Thing y = proc.namedThing("y");
            y.props().types(set(Label.of("person")));

            Traversal.Parameters params = new Traversal.Parameters();
            params.pushValue(_0.id().asVariable(),
                             Predicate.Value.String.of(GraqlToken.Predicate.Equality.EQ),
                             new Traversal.Parameters.Value("Alex"));
            params.pushValue(_1.id().asVariable(),
                             Predicate.Value.String.of(GraqlToken.Predicate.Equality.EQ),
                             new Traversal.Parameters.Value("John"));
            params.pushValue(r1.id().asVariable(),
                             Predicate.Value.Numerical.of(GraqlToken.Predicate.Equality.EQ, PredicateArgument.Value.LONG),
                             new Traversal.Parameters.Value(3L));
            params.pushValue(r2.id().asVariable(),
                             Predicate.Value.Numerical.of(GraqlToken.Predicate.Equality.EQ, PredicateArgument.Value.LONG),
                             new Traversal.Parameters.Value(1L));

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
    }

}

