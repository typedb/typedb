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
import grakn.core.core.Schema;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.atom.predicate.Predicate;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.kb.graql.reasoner.ReasonerException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
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
import static junit.framework.TestCase.assertTrue;

public class AtomicConversionIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session session;

    private static Conjunction<Statement> attributePattern;
    private static Conjunction<Statement> relationPattern;
    private static Conjunction<Statement> implicitRelationPattern;
    private static Variable attributeVar;
    private static Variable relationVar;

    @BeforeClass
    public static void loadContext(){
        final String resourcePath = "test-integration/graql/reasoner/resources/";
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

        String attributeLabel = "resource";
        Statement implicitRel = Graql.var(relationVar)
                .rel(Schema.ImplicitType.HAS_OWNER.getLabel(attributeLabel).getValue(), "x")
                .rel(Schema.ImplicitType.HAS_VALUE.getLabel(attributeLabel).getValue(), "y")
                .isa(Schema.ImplicitType.HAS.getLabel(attributeLabel).getValue());
        Statement xId = Graql.var("x").id("V456");
        Statement yId = Graql.var("y").id("V789");
        implicitRelationPattern = Graql.and(Sets.newHashSet(implicitRel, xId, yId));
    }

    @AfterClass
    public static void finalise(){
        session.close();
    }

    @Test
    public void whenConvertingAttributeToIsaAtom_predicatesArePreserved(){
        try(Transaction tx = session.readTransaction()){
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
        try(Transaction tx = session.readTransaction()){
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
            Atom relation = reasonerQueryFactory.atomic(relationPattern).getAtom();
            IsaAtom isa = relation.toIsaAtom();

            assertEquals(
                    relation.getAllPredicates(relationVar, Predicate.class).collect(toSet()),
                    isa.getAllPredicates(relationVar, Predicate.class).collect(toSet())
            );
        }
    }

    @Test (expected = ReasonerException.class)
    public void whenConvertingNonImplicitRelationToAttribute_weThrow(){
        try(Transaction tx = session.readTransaction()){
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            Atom relation = reasonerQueryFactory.atomic(relationPattern).getAtom();
            AttributeAtom attribute = relation.toAttributeAtom();
            assertEquals(
                    relation.getPredicates().collect(toSet()),
                    attribute.getPredicates().collect(toSet())
            );
        }
    }

    @Test
    public void whenConvertingImplicitRelationToAttribute_predicatesArePreserved(){
        try(Transaction tx = session.readTransaction()){
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            Atom relation = reasonerQueryFactory.atomic(implicitRelationPattern).getAtom();
            AttributeAtom attribute = relation.toAttributeAtom();

            assertEquals(
                    relation.getPredicates().filter(p -> attribute.getVarNames().contains(p.getVarName())).collect(toSet()),
                    attribute.getPredicates().collect(toSet())
            );
        }
    }

    @Test
    public void whenConvertingAttributeToRelation_predicatesArePreserved(){
        try(Transaction tx = session.readTransaction()){
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            Atom attribute = reasonerQueryFactory.atomic(attributePattern).getAtom();
            RelationAtom relation = attribute.toRelationAtom();

            assertEquals(
                    relation.getPredicates(IdPredicate.class).collect(toSet()),
                    attribute.getPredicates(IdPredicate.class).collect(toSet())
            );

            assertEquals(
                    relation.getPredicates(ValuePredicate.class).collect(toSet()),
                    attribute.getInnerPredicates(ValuePredicate.class).collect(toSet())
            );
        }
    }

    @Test
    public void whenPerformingAttributeRelationIdentityConversion_equivalenceIsPreserved(){
        try(Transaction tx = session.readTransaction()){
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            Atom attribute = reasonerQueryFactory.atomic(attributePattern).getAtom();
            RelationAtom intermittentAtom = attribute.toRelationAtom();
            AttributeAtom equivalentAttribute = intermittentAtom.toAttributeAtom();
            assertTrue(attribute.isAlphaEquivalent(equivalentAttribute));
            assertEquals(attribute, equivalentAttribute);
        }
    }

    @Test
    public void whenPerformingRelationAttributeIdentityConversion_equivalenceIsPreserved(){
        try(Transaction tx = session.readTransaction()){
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
            Atom relation = reasonerQueryFactory.atomic(implicitRelationPattern).getAtom();
            AttributeAtom intermittentAtom = relation.toAttributeAtom();
            Atom equivalentRelation = intermittentAtom.toRelationAtom();
            assertEquals(relation, equivalentRelation);
        }
    }
}
