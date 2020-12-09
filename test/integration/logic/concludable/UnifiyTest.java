/*
 * Copyright (C) 2020 Grakn Labs
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.logic.concludable;

import grakn.core.Grakn;
import grakn.core.common.parameters.Arguments;
import grakn.core.logic.LogicManager;
import grakn.core.logic.concludable.ConjunctionConcludable;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import graql.lang.Graql;
import graql.lang.pattern.variable.Reference;
import graql.lang.query.GraqlDefine;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.list;
import static grakn.core.pattern.variable.VariableRegistry.createFromThings;
import static java.nio.charset.StandardCharsets.UTF_8;

public class UnifiyTest {
    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("rule-test");
    private static String database = "rule-test";
    private static RocksTransaction transaction;
    private static Grakn.Session session;

    @BeforeClass
    public static void open_session() throws IOException {
        Util.resetDirectory(directory);

        RocksGrakn grakn = RocksGrakn.open(directory);
        grakn.databases().create(database);
        session = grakn.session(database, Arguments.Session.Type.SCHEMA);
    }

    @AfterClass
    public static void close_session() {
        session.close();
    }

    private static void define_standard_schema() throws IOException {
        transaction = (RocksTransaction) session.transaction(Arguments.Transaction.Type.WRITE);
        final GraqlDefine query = Graql.parseQuery(
                new String(Files.readAllBytes(Paths.get("test/integration/reasoner/basic-schema.gql")), UTF_8));
        transaction.query().define(query);
    }

    private Conjunction parseConjunction(String query) {
        return Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
    }

    private ThingVariable parseThingVariable(String graqlVariable, String variableName) {
        return createFromThings(list(Graql.parseVariable(graqlVariable).asThing())).get(Reference.named(variableName)).asThing();
    }


    //TESTS START
    @Test
    public void unifyIsa() throws IOException {
        define_standard_schema();
        LogicManager logicMgr = transaction.logics();
        String conjunction = "{ $a isa person; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Isa conjConcludable = concludables.iterator().next().asIsa();

        Conjunction headConjunction = parseConjunction("{ $a 5; $a isa age; }");
        ThingVariable variable = parseThingVariable("$a isa age", "a");
        IsaConstraint isaConstraint = variable.isa().get();
        HeadConcludable.Isa isaConcludable = new HeadConcludable.Isa(isaConstraint, headConjunction.variables());

        Stream<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(isaConcludable);




    }

}
