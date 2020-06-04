/*
 * Copyright (C) 2020 Grakn Labs
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
 */

package grakn.core.graql.reasoner.atomic;

import com.google.common.collect.Sets;
import grakn.core.common.config.Config;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.atom.predicate.Predicate;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.kb.graql.reasoner.ReasonerException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestStorage;
import grakn.core.test.rule.SessionUtil;
import grakn.core.test.rule.TestTransactionProvider;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.property.HasAttributeProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;

public class AtomicConversionIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session session;

    private static Conjunction<Statement> attributePattern;
    private static Conjunction<Statement> relationPattern;
    private static Variable attributeVar;
    private static Variable relationVar;

    @BeforeClass
    public static void loadContext(){
        final String resourcePath = "test/integration/graql/reasoner/resources/";
        Config mockServerConfig = storage.createCompatibleServerConfig();
        session = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        loadFromFileAndCommit(resourcePath, "genericSchema.gql", session);

        Statement has = Graql.var("x").has("resource", "b");
        attributeVar = has.getProperties(HasAttributeProperty.class).iterator().next().attribute().var();
        Statement id = Graql.var(attributeVar).id("V123");
        attributePattern = Graql.and(Sets.newHashSet(has, id));

        relationVar = Graql.var("r").var();
        Statement rel = Graql.var(relationVar).rel("baseRole1", "x").rel("baseRole2", "y").isa("binary");
        Statement rId = Graql.var(relationVar).id("V123");
        relationPattern = Graql.and(Sets.newHashSet(rel, rId));
    }

    @AfterClass
    public static void finalise(){
        session.close();
    }

    @Test
    public void whenConvertingAttributeToIsaAtom_predicatesArePreserved(){
        try(Transaction tx = session.transaction(Transaction.Type.READ)){
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
            Atom attribute = reasonerQueryFactory.atomic(attributePattern).getAtom();
            IsaAtom isa = attribute.toIsaAtom();

            assertEquals(
                    attribute.getAllPredicates(attributeVar, Predicate.class).collect(toSet()),
                    isa.getAllPredicates(attributeVar, Predicate.class).collect(toSet())
                    );
        }
    }

    @Test
    public void whenConvertingRelationToIsaAtom_predicatesArePreserved(){
        try(Transaction tx = session.transaction(Transaction.Type.READ)){
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
            Atom relation = reasonerQueryFactory.atomic(relationPattern).getAtom();
            IsaAtom isa = relation.toIsaAtom();

            assertEquals(
                    relation.getAllPredicates(relationVar, Predicate.class).collect(toSet()),
                    isa.getAllPredicates(relationVar, Predicate.class).collect(toSet())
            );
        }
    }
}
