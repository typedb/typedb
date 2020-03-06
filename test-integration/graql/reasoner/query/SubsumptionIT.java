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
 *
 */

package grakn.core.graql.reasoner.query;

import grakn.core.common.config.Config;
import grakn.core.graql.reasoner.graph.GenericSchemaGraph;
import grakn.core.graql.reasoner.pattern.QueryPattern;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.statement.Statement;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class SubsumptionIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session genericSchemaSession;
    private static GenericSchemaGraph genericSchemaGraph;

    @BeforeClass
    public static void loadContext() {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        genericSchemaSession = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        genericSchemaGraph = new GenericSchemaGraph(genericSchemaSession);
    }

    @AfterClass
    public static void closeSession() {
        genericSchemaSession.close();
    }

    @Test
    public void testSubsumption_differentRelationVariants() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentRelationVariants = genericSchemaGraph.differentRelationVariants();
            int[][] subsumptionMatrix = new int[][]{
                    //0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12,13,14,15,16,17
                    {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//0
                    {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},

                    {1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//4
                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},

                    {1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//7
                    {1, 0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0},

                    {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},//11
                    {1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0},

                    {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0},//15
                    {1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0},
                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0},
                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1}
            };

            subsumption(
                    differentRelationVariants.patterns(),
                    differentRelationVariants.patterns(),
                    subsumptionMatrix,
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    @Test
    public void testSubsumption_reflexiveNonReflexiveRelationPairs() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction) tx).reasonerQueryFactory();
            String id = tx.getEntityType("baseRoleEntity").instances().iterator().next().id().getValue();
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction("(baseRole1: $x, baseRole2: $y);"));
            ReasonerAtomicQuery child2 = reasonerQueryFactory.atomic(conjunction("{(baseRole1: $x, baseRole2: $y); $y id " + id + ";};"));
            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction("(baseRole1: $x, baseRole2: $x);"));

            assertFalse(child.isSubsumedBy(parent));
            assertFalse(child2.isSubsumedBy(parent));
            assertFalse(child.isSubsumedBy(parent));
            assertFalse(child2.isSubsumedBy(parent));
        }
    }

    @Test
    public void testSubsumption_differentReflexiveRelationVariants() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            int[][] subsumptionMatrix = new int[][]{
                    //0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12,13,14,15,16,17
                    {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//0
                    {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},

                    {1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0},//4
                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1},

                    {1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0},//7
                    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},

                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0},//11
                    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},

                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1}//15
            };
            subsumption(
                    genericSchemaGraph.differentReflexiveRelationVariants().patterns(),
                    genericSchemaGraph.differentReflexiveRelationVariants().patterns(),
                    subsumptionMatrix, (TestTransactionProvider.TestTransaction) tx);
        }
    }

    @Test
    public void testSubsumption_differentRelationVariantsWithVariableRoles() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentRelationVariantsWithVariableRoles = genericSchemaGraph.differentRelationVariantsWithVariableRoles();

            int[][] subsumptionMatrix = new int[][]{
                    //0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12,13,14,15,16,17
                    {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//0
                    {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},

                    {1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//4
                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0},

                    {1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//7

                    {1, 0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 1, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0},

                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},//11
                    {1, 0, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0},

                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0},//15
                    {1, 0, 0, 0, 1, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0},
                    {1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0},
                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1}
            };

            subsumption(
                    differentRelationVariantsWithVariableRoles.patterns(),
                    differentRelationVariantsWithVariableRoles.patterns(),
                    subsumptionMatrix,
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    @Test
    public void testSubsumption_differentRelationVariants_differentRelationVariantsWithVariableRoles() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentRelationVariants = genericSchemaGraph.differentRelationVariants();
            QueryPattern differentRelationVariantsWithVariableRoles = genericSchemaGraph.differentRelationVariantsWithVariableRoles();

            int[][] subsumptionMatrix = new int[][]{
                    //0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12,13,14,15,16,17
                    {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//0
                    {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},

                    {1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//4
                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0},

                    {1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//7
                    {1, 0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 1, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0},

                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},//11
                    {1, 0, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0},

                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0},//15
                    {1, 0, 0, 0, 1, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0},
                    {1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0},
                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1}
            };
            subsumption(
                    differentRelationVariants.patterns(),
                    differentRelationVariantsWithVariableRoles.patterns(),
                    subsumptionMatrix,
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    @Test
    public void testSubsumption_differentRelationVariantsWithVariableRoles_differentRelationVariants() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentRelationVariants = genericSchemaGraph.differentRelationVariants();
            QueryPattern differentRelationVariantsWithVariableRoles = genericSchemaGraph.differentRelationVariantsWithVariableRoles();
            subsumption(
                    differentRelationVariantsWithVariableRoles.patterns(),
                    differentRelationVariants.patterns(),
                    QueryPattern.zeroMatrix(differentRelationVariantsWithVariableRoles.size(), differentRelationVariants.size()),
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    @Test
    public void testSubsumption_differentRelationVariantsWithMetaRoles() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentRelationVariantsWithMetaRoles = genericSchemaGraph.differentRelationVariantsWithMetaRoles();
            int[][] subsumptionMatrix = new int[][]{
                    //0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12,13,14,15,16,17
                    {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//0
                    {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},

                    {1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//4
                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0},

                    {1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//7
                    {1, 0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 1, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0},

                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},//11
                    {1, 0, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0},

                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0},//15
                    {1, 0, 0, 0, 1, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0},
                    {1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0},
                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1}
            };


            subsumption(
                    differentRelationVariantsWithMetaRoles.patterns(),
                    differentRelationVariantsWithMetaRoles.patterns(),
                    subsumptionMatrix,
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    @Test
    public void testSubsumption_differentRelationVariants_differentRelationVariantsWithMetaRoles() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentRelationVariants = genericSchemaGraph.differentRelationVariants();
            QueryPattern differentRelationVariantsWithMetaRoles = genericSchemaGraph.differentRelationVariantsWithMetaRoles();
            int[][] subsumptionMatrix = new int[][]{
                    //0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12,13,14,15,16,17
                    {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//0
                    {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},

                    {1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//4
                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0},

                    {1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//7
                    {1, 0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 1, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0},

                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},//11
                    {1, 0, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0},

                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0},//15
                    {1, 0, 0, 0, 1, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0},
                    {1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0},
                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1}
            };

            subsumption(
                    differentRelationVariants.patterns(),
                    differentRelationVariantsWithMetaRoles.patterns(),
                    subsumptionMatrix,
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    @Test
    public void testSubsumption_differentRelationVariantsWithMetaRoles_differentRelationVariants() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentRelationVariants = genericSchemaGraph.differentRelationVariants();
            QueryPattern differentRelationVariantsWithMetaRoles = genericSchemaGraph.differentRelationVariantsWithMetaRoles();

            subsumption(
                    differentRelationVariantsWithMetaRoles.patterns(),
                    differentRelationVariants.patterns(),
                    QueryPattern.zeroMatrix(differentRelationVariantsWithMetaRoles.size(), differentRelationVariants.size()),
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    @Test
    public void testSubsumption_differentRelationVariants_differentRelationVariantsWithRelationVariable() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentRelationVariants = genericSchemaGraph.differentRelationVariants();
            QueryPattern differentRelationVariantsWithRelationVariable = genericSchemaGraph.differentRelationVariantsWithRelationVariable();
            int[][] subsumptionMatrix = new int[][]{
                    //0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12,13,14,15,16,17 18
                    {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//0
                    {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},

                    {1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//4
                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,},
                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},

                    {1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//7
                    {1, 0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},

                    {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0},//11
                    {1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0},

                    {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0},//15
                    {1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0},
                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0},
                    {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0},
                    {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0}
            };

            subsumption(
                    differentRelationVariants.patterns(),
                    differentRelationVariantsWithRelationVariable.patterns(),
                    subsumptionMatrix,
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    @Test
    public void testSubsumption_differentRelationVariantsWithRelationVariable_differentRelationVariants() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentRelationVariants = genericSchemaGraph.differentRelationVariants();
            QueryPattern differentRelationVariantsWithRelationVariable = genericSchemaGraph.differentRelationVariantsWithRelationVariable();

            subsumption(
                    differentRelationVariantsWithRelationVariable.patterns(),
                    differentRelationVariants.patterns(),
                    QueryPattern.zeroMatrix(differentRelationVariantsWithRelationVariable.size(), differentRelationVariants.size()),
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    @Test
    public void testSubsumption_differentTypeRelationVariants_differentRelationVariants() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentTypeRelationVariants = genericSchemaGraph.differentTypeRelationVariants();
            QueryPattern differentRelationVariants = genericSchemaGraph.differentRelationVariants();

            subsumption(
                    differentTypeRelationVariants.patterns(),
                    differentRelationVariants.patterns(),
                    QueryPattern.zeroMatrix(differentTypeRelationVariants.size(), differentRelationVariants.size()),
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    @Test
    public void testSubsumption_differentRelationVariants_differentTypeRelationVariants() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentTypeRelationVariants = genericSchemaGraph.differentTypeRelationVariants();
            QueryPattern differentRelationVariants = genericSchemaGraph.differentRelationVariants();

            subsumption(
                    differentRelationVariants.patterns(),
                    differentTypeRelationVariants.patterns(),
                    QueryPattern.zeroMatrix(differentRelationVariants.size(), differentTypeRelationVariants.size()),
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    /**
     * No subsumption relation possible in this case:
     * <p>
     * Child:
     * $x isa relation;
     * <p>
     * Parent:
     * $x ($y, $z) isa relation;
     * <p>
     * Even though it looks like child specialises parent, parent describes a subset of all possible relations - ones that have
     * at least two roleplayers. As a result, parent in general doesn't retain all the answers to child.
     */
    @Test
    public void testSubsumption_differentTypeRelationVariants_differentRelationVariantsWithRelationVariable() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentTypeRelationVariants = genericSchemaGraph.differentTypeRelationVariants();
            QueryPattern differentRelationVariantsWithRelationVariable = genericSchemaGraph.differentRelationVariantsWithRelationVariable();

            subsumption(
                    differentTypeRelationVariants.patterns(),
                    differentRelationVariantsWithRelationVariable.patterns(),
                    QueryPattern.zeroMatrix(differentTypeRelationVariants.size(), differentRelationVariantsWithRelationVariable.size()),
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    @Test
    public void testSubsumption_differentRelationVariantsWithRelationVariable_differentTypeRelationVariants() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentTypeRelationVariants = genericSchemaGraph.differentTypeRelationVariants();
            QueryPattern differentRelationVariantsWithRelationVariable = genericSchemaGraph.differentRelationVariantsWithRelationVariable();

            subsumption(
                    differentRelationVariantsWithRelationVariable.patterns(),
                    differentTypeRelationVariants.patterns(),
                    QueryPattern.zeroMatrix(differentRelationVariantsWithRelationVariable.size(), differentTypeRelationVariants.size()),
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    @Test
    public void testSubsumption_differentResourceVariants() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentResourceVariants = genericSchemaGraph.differentResourceVariants();
            subsumption(
                    differentResourceVariants.patterns(),
                    differentResourceVariants.patterns(),
                    differentResourceVariants.subsumptionMatrix(),
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    @Test
    public void testSubsumption_differentTypeResourceVariants() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentTypeVariants = genericSchemaGraph.differentTypeResourceVariants();
            int[][] subsumptionMatrix = new int[][]{
                    //0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12,13,14,15,16,17
                    {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//0
                    {1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//4
                    {1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//5

                    {1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//6
                    {1, 1, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//7
                    {1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},

                    {1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0},//9
                    {1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0},

                    {1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},//11
                    {1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0},

                    {1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1},//13
                    {1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 1},

                    {1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0},//15
                    {1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1},
                    {1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0},
                    {1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
            };

            subsumption(
                    differentTypeVariants.patterns(),
                    differentTypeVariants.patterns(),
                    subsumptionMatrix,
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    /**
     * No subsumption relation possible in this case:
     * <p>
     * Child:
     * $x isa resource;
     * <p>
     * Parent:
     * $x has resource $r;
     * <p>
     * Equivalent to:
     * <p>
     * $r isa resource; (resource-owner:$x, resource-value: $r) isa @...;
     * <p>
     * Even though it looks like child specialises parent, parent describes connected attributes, whereas
     * child describes all attributes including disconnected ones. As a result, parent in general doesn't retain
     * all the answers to child.
     */
    @Test
    public void testSubsumption_differentTypeResourceVariants_differentResourceVariants() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentTypeVariants = genericSchemaGraph.differentTypeResourceVariants();
            QueryPattern differentResourceVariants = genericSchemaGraph.differentResourceVariants();

            subsumption(
                    differentTypeVariants.patterns(),
                    differentResourceVariants.patterns(),
                    QueryPattern.zeroMatrix(differentTypeVariants.size(), differentResourceVariants.size()),
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    @Test
    public void testSubsumption_differentResourceVariants_differentTypeResourceVariants() {
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            QueryPattern differentTypeVariants = genericSchemaGraph.differentTypeResourceVariants();
            QueryPattern differentResourceVariants = genericSchemaGraph.differentResourceVariants();

            subsumption(
                    differentResourceVariants.patterns(),
                    differentTypeVariants.patterns(),
                    QueryPattern.zeroMatrix(differentResourceVariants.size(), differentTypeVariants.size()),
                    (TestTransactionProvider.TestTransaction) tx
            );
        }
    }

    private void subsumption(List<String> children, List<String> parents, int[][] resultMatrix, TestTransactionProvider.TestTransaction tx) {
        int i = 0;
        int j = 0;
        for (String child : children) {
            for (String parent : parents) {
                subsumption(child, parent, resultMatrix[i][j] == 1, tx);
                j++;
            }
            i++;
            j = 0;
        }
    }

    private void subsumption(String childString, String parentString, boolean isSubsumedBy, TestTransactionProvider.TestTransaction tx) {
        ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
        ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childString));
        ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentString));
        UnifierType unifierType = UnifierType.SUBSUMPTIVE;

        assertEquals("Unexpected subsumption outcome: between the child - parent pair:\n" + child + " :\n" + parent + "\n", isSubsumedBy, child.isSubsumedBy(parent));
        MultiUnifier multiUnifier = child.getMultiUnifier(parent, unifierType);
        if (isSubsumedBy) {
            boolean queriesEquivalent = child.isEquivalent(parent);
            assertEquals("Unexpected inverse subsumption outcome: between the child - parent pair:\n" + parent + " :\n" + child + "\n", queriesEquivalent, parent.isSubsumedBy(child));
            MultiUnifier multiUnifierInverse = parent.getMultiUnifier(child, unifierType);
            if (queriesEquivalent) assertEquals(multiUnifierInverse, multiUnifier.inverse());
        }
    }

    private Conjunction<Statement> conjunction(String patternString) {
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }
}
