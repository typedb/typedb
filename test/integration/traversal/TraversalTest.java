/*
 * Copyright (C) 2022 Vaticle
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
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreSession;
import com.vaticle.typedb.core.database.CoreTransaction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Modifiers;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.predicate.Predicate;
import com.vaticle.typedb.core.traversal.predicate.PredicateArgument;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;
import com.vaticle.typedb.core.traversal.procedure.ProcedureVertex;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.common.TypeQLToken;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.parameters.Arguments.Transaction.Type.READ;
import static com.vaticle.typedb.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;
import static com.vaticle.typedb.core.common.parameters.Order.Desc.DESC;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TraversalTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("query-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageIndexCacheSize(MB).storageDataCacheSize(MB);
    private static final String database = "query-test";

    private static CoreDatabaseManager databaseMgr;
    private static CoreSession session;

    @Before
    public void setup() throws IOException {
        Util.resetDirectory(dataDir);
        databaseMgr = CoreDatabaseManager.open(options);
        databaseMgr.create(database);
        session = databaseMgr.session(database, Arguments.Session.Type.SCHEMA);
    }

    @After
    public void teardown() {
        databaseMgr.close();
    }

    @Test
    public void sorting_longs() {
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            TypeQLDefine query = TypeQL.parseQuery("define person sub entity, owns age; age sub attribute, value long;");
            transaction.query().define(query);
            transaction.commit();
        }
        session.close();
        session = databaseMgr.session(database, Arguments.Session.Type.DATA);
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has age 10;").asInsert());
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has age 11;").asInsert());
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has age 12;").asInsert());
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has age 13;").asInsert());
            transaction.commit();
        }
        try (CoreTransaction transaction = session.transaction(READ)) {
            Disjunction disjunction = Disjunction.create(TypeQL.parseQuery("match $x isa person, has age $a;")
                    .asMatch().conjunction().normalise());
            transaction.logic().typeInference().applyCombination(disjunction);
            Identifier.Variable.Name var = Identifier.Variable.name("a");
            Modifiers.Filter filter = Modifiers.Filter.create(set(var));
            Modifiers.Sorting sorting = Modifiers.Sorting.create(singletonList(var), map(pair(var, DESC)));
            SortedIterator<ConceptMap.Sortable, Order.Asc> answers = transaction.reasoner().executeTraversalSorted(disjunction, filter, sorting);

            long lastValue = Long.MAX_VALUE;
            while (answers.hasNext()) {
                ConceptMap.Sortable answer = answers.next();
                long nextValue = answer.get("a").asAttribute().asLong().getValue();
                assertTrue(nextValue <= lastValue);
                lastValue = nextValue;
            }
            ;
        }
    }

    @Test
    public void sorting_booleans() {
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            TypeQLDefine query = TypeQL.parseQuery("define person sub entity, owns is-married; is-married sub attribute, value boolean;");
            transaction.query().define(query);
            transaction.commit();
        }
        session.close();
        session = databaseMgr.session(database, Arguments.Session.Type.DATA);
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has is-married true;").asInsert());
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has is-married false;").asInsert());
            transaction.commit();
        }
        try (CoreTransaction transaction = session.transaction(READ)) {
            Disjunction disjunction = Disjunction.create(TypeQL.parseQuery("match $x isa person, has is-married $a;")
                    .asMatch().conjunction().normalise());
            transaction.logic().typeInference().applyCombination(disjunction);
            Identifier.Variable.Name var = Identifier.Variable.name("a");
            Modifiers.Filter filter = Modifiers.Filter.create(set(var));
            Modifiers.Sorting sorting = Modifiers.Sorting.create(singletonList(var), map(pair(var, DESC)));
            List<ConceptMap.Sortable> answers = transaction.reasoner().executeTraversalSorted(disjunction, filter, sorting).toList();
            assertEquals(true, answers.get(0).get("a").asAttribute().asBoolean().getValue());
            assertEquals(false, answers.get(1).get("a").asAttribute().asBoolean().getValue());
        }
    }

    @Test
    public void sorting_doubles() {
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            TypeQLDefine query = TypeQL.parseQuery("define person sub entity, owns money; money sub attribute, value double;");
            transaction.query().define(query);
            transaction.commit();
        }
        session.close();
        session = databaseMgr.session(database, Arguments.Session.Type.DATA);
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has money 10.3;").asInsert());
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has money 11.5;").asInsert());
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has money 12.1999;").asInsert());
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has money 13.2000;").asInsert());
            transaction.commit();
        }
        try (CoreTransaction transaction = session.transaction(READ)) {
            Disjunction disjunction = Disjunction.create(TypeQL.parseQuery("match $x isa person, has money $a;")
                    .asMatch().conjunction().normalise());
            transaction.logic().typeInference().applyCombination(disjunction);
            Identifier.Variable.Name var = Identifier.Variable.name("a");
            Modifiers.Filter filter = Modifiers.Filter.create(set(var));
            Modifiers.Sorting sorting = Modifiers.Sorting.create(singletonList(var), map(pair(var, ASC)));
            SortedIterator<ConceptMap.Sortable, Order.Asc> answers = transaction.reasoner().executeTraversalSorted(disjunction, filter, sorting);

            double lastValue = Double.MIN_VALUE;
            while (answers.hasNext()) {
                ConceptMap.Sortable answer = answers.next();
                double nextValue = answer.get("a").asAttribute().asDouble().getValue();
                assertTrue(nextValue >= lastValue);
                lastValue = nextValue;
            }
            ;
        }
    }

    @Test
    public void sorting_dates() {
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            TypeQLDefine query = TypeQL.parseQuery("define person sub entity, owns birth-date; birth-date sub attribute, value datetime;");
            transaction.query().define(query);
            transaction.commit();
        }
        session.close();
        session = databaseMgr.session(database, Arguments.Session.Type.DATA);
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has birth-date 2000-01-01T00:00;").asInsert());
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has birth-date 2001-01-01T00:00;").asInsert());
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has birth-date 2000-02-03T00:00;").asInsert());
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has birth-date 2000-01-04T00:00;").asInsert());
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has birth-date 2000-01-04T01:00;").asInsert());
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has birth-date 2000-01-04T00:01;").asInsert());
            transaction.commit();
        }
        try (CoreTransaction transaction = session.transaction(READ)) {
            Disjunction disjunction = Disjunction.create(TypeQL.parseQuery("match $x isa person, has birth-date $a;")
                    .asMatch().conjunction().normalise());
            transaction.logic().typeInference().applyCombination(disjunction);
            Identifier.Variable.Name var = Identifier.Variable.name("a");
            Modifiers.Filter filter = Modifiers.Filter.create(set(var));
            Modifiers.Sorting sorting = Modifiers.Sorting.create(singletonList(var), map(pair(var, DESC)));
            SortedIterator<ConceptMap.Sortable, Order.Asc> answers = transaction.reasoner().executeTraversalSorted(disjunction, filter, sorting);

            LocalDateTime lastValue = LocalDateTime.MAX;
            while (answers.hasNext()) {
                ConceptMap.Sortable answer = answers.next();
                LocalDateTime nextValue = answer.get("a").asAttribute().asDateTime().getValue();
                assertTrue(nextValue.compareTo(lastValue) < 0);
                lastValue = nextValue;
            }
            ;
        }
    }

    @Test
    public void backward_isa_edge_fetches_supertypes() {
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            TypeQLDefine query = TypeQL.parseQuery("define person sub entity;");
            transaction.query().define(query);
            transaction.commit();
        }
        session.close();

        session = databaseMgr.session(database, Arguments.Session.Type.DATA);
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            transaction.query().insert(TypeQL.parseQuery("insert $x isa person;").asInsert());
            transaction.commit();
        }
        try (CoreTransaction transaction = session.transaction(READ)) {
            /*
            match $x isa $type;
            */
            /*
            Edges:
            0: $type [type] { labels: [entity, person, thing], abstract: false, value: [], regex: null } (start)
            1: $x [thing] { hasIID: false, types: [person], predicates: [] } (end)
                ($type <--[ISA]--* $x) { isTransitive: true }
            */
            GraphProcedure.Builder proc = new GraphProcedure.Builder();

            ProcedureVertex.Type type = proc.namedType(0, "type");
            type.props().labels(set(Label.of("person"), Label.of("entity"), Label.of("thing")));

            ProcedureVertex.Thing x = proc.namedThing(1, "x");
            x.props().types(set(Label.of("person")));

            proc.backwardIsa(type, x, true);

            Traversal.Parameters params = new Traversal.Parameters();
            Modifiers modifiers = new Modifiers();

            Set<Identifier.Variable.Retrievable> filter = set(
                    x.id().asVariable().asRetrievable(),
                    type.id().asVariable().asRetrievable()
            );
            modifiers.filter(Modifiers.Filter.create(filter));

            GraphProcedure procedure = proc.build();
            FunctionalIterator<VertexMap> vertices = procedure.iterator(transaction.traversal().graph(), params, modifiers);
            assertEquals(3, vertices.count());
        }
    }

    @Test
    public void roleplayer_scoping_test_1() {
        preparePostsSchemaAndData();
        try (CoreTransaction transaction = session.transaction(READ)) {
            /*
            Edges:
            0: $_0 [thing] { hasIID: false, types: [reply-of], predicates: [] } (start)
            1: $s [thing] { hasIID: false, types: [post], predicates: [] }
                    1: ($_0 *--[ROLEPLAYER]--> $s) { roleTypes: [reply-of:reply] }
            2: $p [thing] { hasIID: false, types: [post], predicates: [] }
                    2: ($_0 *--[ROLEPLAYER]--> $p) { roleTypes: [reply-of:original] }
            3: $d1 [thing] { hasIID: false, types: [creation-date], predicates: [] }
                    3: ($s *--[HAS]--> $d1)
            4: $_1 [thing] { hasIID: false, types: [reply-of], predicates: [] }
                    4: ($p <--[ROLEPLAYER]--* $_1) { roleTypes: [reply-of:original] }
            5: $d2 [thing] { hasIID: false, types: [creation-date], predicates: [] }
                    5: ($d1 *--[< <var>]--> $d2)
            6: $r [thing] { hasIID: false, types: [post], predicates: [] } (end)
                    7: ($d2 <--[HAS]--* $r)
                    6: ($_1 *--[ROLEPLAYER]--> $r) { roleTypes: [reply-of:reply] }
            */

            GraphProcedure.Builder proc = new GraphProcedure.Builder();
            ProcedureVertex.Thing _0 = proc.anonymousThing(0, 0);
            _0.props().types(set(Label.of("reply-of")));

            ProcedureVertex.Thing s = proc.namedThing(1, "s");
            s.props().types(set(Label.of("post")));

            ProcedureVertex.Thing p = proc.namedThing(2, "p");
            p.props().types(set(Label.of("post")));

            ProcedureVertex.Thing d1 = proc.namedThing(3, "d1");
            d1.props().types(set(Label.of("creation-date")));

            ProcedureVertex.Thing _1 = proc.anonymousThing(4, 1);
            _1.props().types(set(Label.of("reply-of")));

            ProcedureVertex.Thing d2 = proc.namedThing(5, "d2");
            d2.props().types(set(Label.of("creation-date")));

            ProcedureVertex.Thing r = proc.namedThing(6, "r");
            r.props().types(set(Label.of("post")));

            proc.forwardRolePlayer(_0, s, 0, set(Label.of("reply", "reply-of")));
            proc.forwardRolePlayer(_0, p, 0, set(Label.of("original", "reply-of")));
            proc.forwardHas(s, d1);
            proc.backwardRolePlayer(p, _1, 0, set(Label.of("original", "reply-of")));
            proc.forwardPredicate(d1, d2, Predicate.Variable.of(TypeQLToken.Predicate.Equality.LT));
            proc.backwardHas(d2, r);
            proc.forwardRolePlayer(_1, r, 0, set(Label.of("reply", "reply-of")));

            Traversal.Parameters params = new Traversal.Parameters();
            Modifiers modifiers = new Modifiers();

            Set<Identifier.Variable.Retrievable> filter = set(
                    r.id().asVariable().asRetrievable(),
                    s.id().asVariable().asRetrievable(),
                    d1.id().asVariable().asRetrievable(),
                    d2.id().asVariable().asRetrievable()
            );
            modifiers.filter(Modifiers.Filter.create(filter));

            GraphProcedure procedure = proc.build();
            FunctionalIterator<VertexMap> vertices = procedure.iterator(transaction.traversal().graph(), params, modifiers);
            assertEquals(10, vertices.count());
        }
    }

    @Test
    public void roleplayer_scoping_test_2() {
        preparePostsSchemaAndData();
        try (CoreTransaction transaction = session.transaction(READ)) {
            /*
            Edges:
            0: $_0 [thing] { hasIID: false, types: [reply-of], predicates: [] } (start)
            1: $p [thing] { hasIID: false, types: [post], predicates: [] }
                    1: ($_0 *--[ROLEPLAYER]--> $p) { roleTypes: [reply-of:original] }
            2: $_1 [thing] { hasIID: false, types: [reply-of], predicates: [] }
                    2: ($p <--[ROLEPLAYER]--* $_1) { roleTypes: [reply-of:original] }
            3: $r [thing] { hasIID: false, types: [post], predicates: [] }
                    3: ($_1 *--[ROLEPLAYER]--> $r) { roleTypes: [reply-of:reply] }
            4: $s [thing] { hasIID: false, types: [post], predicates: [] }
                    4: ($_0 *--[ROLEPLAYER]--> $s) { roleTypes: [reply-of:reply] }
            5: $d1 [thing] { hasIID: false, types: [creation-date], predicates: [] }
                    6: ($s *--[HAS]--> $d1)
            6: $d2 [thing] { hasIID: false, types: [creation-date], predicates: [] } (end)
                    5: ($r *--[HAS]--> $d2)
                    7: ($d1 *--[< <var>]--> $d2)
            */

            GraphProcedure.Builder proc = new GraphProcedure.Builder();
            ProcedureVertex.Thing _0 = proc.anonymousThing(0, 0);
            _0.props().types(set(Label.of("reply-of")));

            ProcedureVertex.Thing p = proc.namedThing(1, "p");
            p.props().types(set(Label.of("post")));

            ProcedureVertex.Thing _1 = proc.anonymousThing(2, 1);
            _1.props().types(set(Label.of("reply-of")));

            ProcedureVertex.Thing r = proc.namedThing(3, "r");
            r.props().types(set(Label.of("post")));

            ProcedureVertex.Thing s = proc.namedThing(4, "s");
            s.props().types(set(Label.of("post")));

            ProcedureVertex.Thing d1 = proc.namedThing(5, "d1");
            d1.props().types(set(Label.of("creation-date")));

            ProcedureVertex.Thing d2 = proc.namedThing(6, "d2");
            d2.props().types(set(Label.of("creation-date")));

            proc.forwardRolePlayer(_0, p, 0, set(Label.of("original", "reply-of")));
            proc.backwardRolePlayer(p, _1, 0, set(Label.of("original", "reply-of")));
            proc.forwardRolePlayer(_1, r, 0, set(Label.of("reply", "reply-of")));
            proc.forwardRolePlayer(_0, s, 0, set(Label.of("reply", "reply-of")));
            proc.forwardHas(s, d1);
            proc.forwardHas(r, d2);
            proc.forwardPredicate(d1, d2, Predicate.Variable.of(TypeQLToken.Predicate.Equality.LT));

            Traversal.Parameters params = new Traversal.Parameters();
            Modifiers modifiers = new Modifiers();

            Set<Identifier.Variable.Retrievable> filter = set(
                    r.id().asVariable().asRetrievable(),
                    s.id().asVariable().asRetrievable(),
                    d1.id().asVariable().asRetrievable(),
                    d2.id().asVariable().asRetrievable()
            );
            modifiers.filter(Modifiers.Filter.create(filter));

            GraphProcedure procedure = proc.build();
            FunctionalIterator<VertexMap> vertices = procedure.iterator(transaction.traversal().graph(), params, modifiers);
            assertEquals(10, vertices.count());
        }
    }

    private void preparePostsSchemaAndData() {
        try (CoreTransaction transaction = session.transaction(WRITE)) {
            TypeQLDefine query = TypeQL.parseQuery(
                    "define " +
                            "post sub entity," +
                            "    plays reply-of:original," +
                            "    plays reply-of:reply," +
                            "    plays message-succession:predecessor," +
                            "    plays message-succession:successor," +
                            "    owns creation-date;" +
                            "reply-of sub relation," +
                            "    relates original," +
                            "    relates reply;" +
                            "message-succession sub relation," +
                            "    relates predecessor," +
                            "    relates successor;" +
                            "creation-date sub attribute, value datetime;"
            );
            transaction.query().define(query);
            transaction.commit();
        }
        session.close();

        session = databaseMgr.session(database, Arguments.Session.Type.DATA);

        try (CoreTransaction transaction = session.transaction(WRITE)) {
            transaction.query().insert(TypeQL.parseQuery(
                    "insert " +
                            "$x isa post, has creation-date 2020-07-01;" +
                            "$x1 isa post, has creation-date 2020-07-02;" +
                            "$x2 isa post, has creation-date 2020-07-03;" +
                            "$x3 isa post, has creation-date 2020-07-04;" +
                            "$x4 isa post, has creation-date 2020-07-05;" +
                            "$x5 isa post, has creation-date 2020-07-06;" +
                            "(original:$x, reply:$x1) isa reply-of;" +
                            "(original:$x, reply:$x2) isa reply-of;" +
                            "(original:$x, reply:$x3) isa reply-of;" +
                            "(original:$x, reply:$x4) isa reply-of;" +
                            "(original:$x, reply:$x5) isa reply-of;").asInsert()
            );
            transaction.commit();
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
            GraphProcedure.Builder proc = new GraphProcedure.Builder();
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

            ProcedureVertex.Thing n = proc.namedThing(0, "n");
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
            proc.backwardRolePlayer(x, refl, 0, set(Label.of("friend", "friendship")));
            proc.backwardRolePlayer(x, refl, 1, set(Label.of("friend", "friendship")));
            proc.backwardRolePlayer(x, f1, 0, set(Label.of("friend", "friendship")));
            proc.forwardHas(y, _1);
            proc.backwardRolePlayer(y, f1, 1, set(Label.of("friend", "friendship")));
            proc.forwardHas(refl, r1);
            proc.forwardHas(f1, r2);

            Modifiers modifiers = new Modifiers();
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
            modifiers.filter(Modifiers.Filter.create(filter));

            GraphProcedure procedure = proc.build();
            FunctionalIterator<VertexMap> vertices = procedure.iterator(transaction.traversal().graph(), params, modifiers);
            vertices.next();
        }
        session.close();
    }
}
