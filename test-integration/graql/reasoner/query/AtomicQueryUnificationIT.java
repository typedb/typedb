/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.reasoner.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.Entity;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.Relationship;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.Graql;
import grakn.core.graql.Query;
import grakn.core.graql.Var;
import grakn.core.graql.admin.Conjunction;
import grakn.core.graql.admin.MultiUnifier;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.pattern.Patterns;
import grakn.core.graql.internal.query.answer.ConceptMapImpl;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryEquivalence;
import grakn.core.graql.internal.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.internal.reasoner.unifier.UnifierType;
import grakn.core.graql.reasoner.query.pattern.RelationPattern;
import grakn.core.graql.reasoner.query.pattern.ResourcePattern;
import grakn.core.graql.reasoner.query.pattern.TestQueryPattern;
import grakn.core.graql.reasoner.query.pattern.TypePattern;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.session.TransactionImpl;
import grakn.core.server.session.SessionImpl;
import grakn.core.rule.GraknTestServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.graql.Graql.var;
import static grakn.core.graql.reasoner.query.pattern.TestQueryPattern.subList;
import static grakn.core.graql.reasoner.query.pattern.TestQueryPattern.subListExcluding;
import static grakn.core.graql.reasoner.query.pattern.TestQueryPattern.subListExcludingElements;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.collections.CollectionUtils.isEqualCollection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class AtomicQueryUnificationIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl genericSchemaSession;
    private static SessionImpl unificationWithTypesSession;

    private static void loadFromFile(String fileName, Session session){
        try {
            InputStream inputStream = AtomicQueryUnificationIT.class.getClassLoader().getResourceAsStream("test-integration/graql/reasoner/resources/"+fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            tx.graql().parser().parseList(s).forEach(Query::execute);
            tx.commit();
        } catch (Exception e){
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    private static TestQueryPattern differentRelationVariants;
    private static TestQueryPattern differentRelationVariantsWithMetaRoles;
    private static TestQueryPattern differentRelationVariantsWithRelationVariable;

    private static TestQueryPattern differentResourceVariants;
    private static TestQueryPattern differentTypeVariants;


    @BeforeClass
    public static void loadContext(){
        genericSchemaSession = server.sessionWithNewKeyspace();
        loadFromFile("genericSchema.gql", genericSchemaSession);
        unificationWithTypesSession = server.sessionWithNewKeyspace();
        loadFromFile("unificationWithTypesTest.gql", unificationWithTypesSession);

        try(Transaction tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            EntityType subRoleEntityType = tx.getEntityType("subRoleEntity");
            Iterator<Entity> entities = tx.getEntityType("baseRoleEntity").instances()
                    .filter(et -> !et.type().equals(subRoleEntityType) )
                    .collect(toSet()).iterator();
            Entity entity = entities.next();
            Entity anotherEntity = entities.next();
            Entity anotherBaseEntity = tx.getEntityType("anotherBaseRoleEntity").instances().findFirst().orElse(null);
            Entity subEntity = subRoleEntityType.instances().findFirst().orElse(null);
            Iterator<Relationship> relations = tx.getRelationshipType("baseRelation").subs().flatMap(RelationshipType::instances).iterator();
            Relationship relation = relations.next();
            Relationship anotherRelation = relations.next();
            Iterator<Attribute<Object>> resources = tx.getAttributeType("resource").instances().collect(toSet()).iterator();
            Attribute<Object> resource = resources.next();
            Attribute<Object> anotherResource = resources.next();
            System.out.println(entity);
            System.out.println(anotherBaseEntity);
            System.out.println(subEntity);

            differentTypeVariants = new TypePattern(entity.id(), anotherEntity.id());
            differentResourceVariants = new ResourcePattern(entity.id(), anotherEntity.id(), resource.id(), anotherResource.id());

            differentRelationVariants = new RelationPattern(
                    ImmutableMultimap.of(
                            Label.of("baseRole1"), Label.of("baseRoleEntity"),
                            Label.of("baseRole2"), Label.of("anotherBaseRoleEntity")
                    ),
                    Lists.newArrayList(entity.id(), anotherBaseEntity.id(), subEntity.id()),
                    new ArrayList<>()
            ){
                @Override
                public int[][] structuralMatrix() {
                    return new int[][]{
                            {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//0
                            {0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//3
                            {0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0},//7
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},
                            {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0},//11
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},//14
                            {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1}
                    };
                }

                @Override
                public int[][] ruleMatrix() {
                    return new int[][]{
                            //0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12,13,14,15,16,17
                            {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},//0
                            {1, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0},
                            {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1},

                            {1, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0},//3
                            {1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0},
                            {1, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0},
                            {1, 0, 1, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1},

                            {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0},//7
                            {1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {1, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {1, 0, 1, 0, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0},

                            {1, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0},//11
                            {1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0},
                            {1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0},
                            {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0},

                            {1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1},//15
                            {1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0},
                            {1, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0},
                            {1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1}
                    };
                }
            };

            differentRelationVariantsWithMetaRoles = new RelationPattern(
                    ImmutableMultimap.of(
                            Label.of("role"), Label.of("baseRoleEntity"),
                            Label.of("role"), Label.of("anotherBaseRoleEntity")
                    ),
                    Lists.newArrayList(entity.id(), anotherBaseEntity.id(), subEntity.id()),
                    new ArrayList<>()
            ) {

                @Override
                public int[][] exactMatrix(){
                    return new int[][]{
                            {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//0
                            {0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//3
                            {0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0},
                            {0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//7
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0},
                            {0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},//11
                            {0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0},//14
                            {0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
                    };
                }
                @Override
                public int[][] structuralMatrix() {
                    return new int[][]{
                            {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//0
                            {0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//3
                            {0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0},
                            {0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0},
                            {0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0},
                            {0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0},//7
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},
                            {0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0},//11
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},//14
                            {0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1}
                    };
                }

                @Override
                public int[][] ruleMatrix() {
                    return new int[][]{
                            //0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12,13,14,15,16,17
                            {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},//0
                            {1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 1, 1, 1, 0, 1, 0},
                            {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1},

                            {1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0},//3
                            {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0},
                            {1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 1, 1, 1, 0, 1, 0},
                            {1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 1},

                            {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0},//7
                            {1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0},
                            {1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0},

                            {1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 1, 1, 1, 0, 1, 0},//11
                            {1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0},
                            {1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0},
                            {1, 1, 1, 1, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0},

                            {1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 1},//15
                            {1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0},
                            {1, 1, 1, 1, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0},
                            {1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1}
                    };
                }
            };

            differentRelationVariantsWithRelationVariable = new RelationPattern(
                    ImmutableMultimap.of(
                            Label.of("baseRole1"), Label.of("baseRoleEntity"),
                            Label.of("baseRole2"), Label.of("anotherBaseRoleEntity")
                    ),
                    Lists.newArrayList(entity.id(), anotherBaseEntity.id(), subEntity.id()),
                    Lists.newArrayList(relation.id(), anotherRelation.id())
            ){
                @Override
                public int[][] structuralMatrix() {
                    return new int[][]{
                            {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//0
                            {0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//3
                            {0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0},//7
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0},//11
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0, 0},//14
                            {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0, 0},
                            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1},
                            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1}
                    };
                }

                @Override
                public int[][] ruleMatrix() {
                    return new int[][]{
                            //0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12,13,14,15,16,17
                            {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},//0
                            {1, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1},
                            {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1},

                            {1, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 1},//3
                            {1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1},
                            {1, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1},
                            {1, 0, 1, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1},

                            {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1},//7
                            {1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1},
                            {1, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1},
                            {1, 0, 1, 0, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1},

                            {1, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1},//11
                            {1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1},
                            {1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 1},
                            {1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1},

                            {1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1},//15
                            {1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1},
                            {1, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 1},
                            {1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1},
                            {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0},
                            {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1}
                    };
                }
            };
        }
    }

    @AfterClass
    public static void closeSession(){
        genericSchemaSession.close();
        unificationWithTypesSession.close();
    }

    @Test
    public void testUnification_RULE_BinaryRelationWithSubs(){
        try( TransactionImpl tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {

            Concept x1 = getConceptByResourceValue(tx, "x1");
            Concept x2 = getConceptByResourceValue(tx, "x2");

            ReasonerAtomicQuery xbaseQuery = ReasonerQueries.atomic(conjunction("{($x1, $x2) isa binary;}"), tx);
            ReasonerAtomicQuery ybaseQuery = ReasonerQueries.atomic(conjunction("{($y1, $y2) isa binary;}"), tx);

            ConceptMap xAnswer = new ConceptMapImpl(ImmutableMap.of(var("x1"), x1, var("x2"), x2));
            ConceptMap flippedXAnswer = new ConceptMapImpl(ImmutableMap.of(var("x1"), x2, var("x2"), x1));

            ConceptMap yAnswer = new ConceptMapImpl(ImmutableMap.of(var("y1"), x1, var("y2"), x2));
            ConceptMap flippedYAnswer = new ConceptMapImpl(ImmutableMap.of(var("y1"), x2, var("y2"), x1));

            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(xbaseQuery, xAnswer);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(xbaseQuery, flippedXAnswer);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("x1"), var("x2"),
                    var("x2"), var("x1")
            ));
            assertEquals(correctUnifier, unifier);

            ReasonerAtomicQuery yChildQuery = ReasonerQueries.atomic(ybaseQuery, yAnswer);
            ReasonerAtomicQuery yChildQuery2 = ReasonerQueries.atomic(ybaseQuery, flippedYAnswer);

            MultiUnifier unifier2 = yChildQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier2 = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("y1"), var("x1"),
                    var("y2"), var("x2")
            ));
            assertEquals(correctUnifier2, unifier2);

            MultiUnifier unifier3 = yChildQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier3 = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("y1"), var("x2"),
                    var("y2"), var("x1")
            ));
            assertEquals(correctUnifier3, unifier3);
        }
    }

    @Test //only a single unifier exists
    public void testUnification_EXACT_BinaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        try( TransactionImpl tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{$x1 isa twoRoleEntity;($x1, $x2) isa binary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{$y1 isa twoRoleEntity;($y1, $y2) isa binary;}"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("y1"), var("x1"),
                    var("y2"), var("x2")
            ));
            assertTrue(unifier.equals(correctUnifier));
        }
    }

    @Test //only a single unifier exists
    public void testUnification_EXACT_BinaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes(){
        try( TransactionImpl tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{$x1 isa twoRoleEntity;$x2 isa twoRoleEntity2;($x1, $x2) isa binary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{$y1 isa twoRoleEntity;$y2 isa twoRoleEntity2;($y1, $y2) isa binary;}"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("y1"), var("x1"),
                    var("y2"), var("x2")
            ));
            assertEquals(correctUnifier, unifier);
        }
    }

    @Test
    public void testUnification_EXACT_TernaryRelation_ParentRepeatsRoles(){
        try( TransactionImpl tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{(role1: $x, role1: $y, role2: $z) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role3: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role2: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $v, role2: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $u, role2: $q) isa ternary;}"), tx);

            MultiUnifier emptyUnifier = childQuery.getMultiUnifier(parentQuery);
            MultiUnifier emptyUnifier2 = childQuery2.getMultiUnifier(parentQuery);
            MultiUnifier emptyUnifier3 = childQuery4.getMultiUnifier(parentQuery);

            assertEquals(MultiUnifierImpl.nonExistent(), emptyUnifier);
            assertEquals(MultiUnifierImpl.nonExistent(), emptyUnifier2);
            assertEquals(MultiUnifierImpl.nonExistent(), emptyUnifier3);

            MultiUnifier unifier = childQuery3.getMultiUnifier(parentQuery);
            MultiUnifier correctUnifier = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            var("u"), var("x"),
                            var("v"), var("y"),
                            var("q"), var("z")),
                    ImmutableMultimap.of(
                            var("u"), var("y"),
                            var("v"), var("x"),
                            var("q"), var("z"))
            );
            assertEquals(correctUnifier, unifier);
            assertEquals(2, unifier.size());
        }
    }

    @Test
    public void testUnification_EXACT_TernaryRelation_ParentRepeatsMetaRoles_ParentRepeatsRPs(){
        try( TransactionImpl tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{(role: $x, role: $x, role2: $y) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role3: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role2: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $v, role2: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $u, role2: $q) isa ternary;}"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            var("q"), var("x"),
                            var("u"), var("x"),
                            var("v"), var("y"))
            );
            assertEquals(correctUnifier, unifier);

            MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier2 = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            var("u"), var("x"),
                            var("q"), var("x"),
                            var("v"), var("y")),
                    ImmutableMultimap.of(
                            var("u"), var("x"),
                            var("v"), var("x"),
                            var("q"), var("y"))
            );
            assertEquals(correctUnifier2, unifier2);
            assertEquals(unifier2.size(), 2);

            MultiUnifier unifier3 = childQuery3.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier3 = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            var("u"), var("x"),
                            var("v"), var("x"),
                            var("q"), var("y"))
            );
            assertEquals(correctUnifier3, unifier3);

            MultiUnifier unifier4 = childQuery4.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier4 = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("u"), var("x"),
                    var("q"), var("y")
            ));
            assertEquals(correctUnifier4, unifier4);
        }
    }

    @Test
    public void testUnification_EXACT_TernaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        try( TransactionImpl tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{$x1 isa threeRoleEntity;$x3 isa threeRoleEntity3;($x1, $x2, $x3) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{$y3 isa threeRoleEntity3;$y1 isa threeRoleEntity;($y2, $y3, $y1) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{$y3 isa threeRoleEntity3;$y2 isa threeRoleEntity2;$y1 isa threeRoleEntity;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.EXACT);
            MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.EXACT);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("y1"), var("x1"),
                    var("y2"), var("x2"),
                    var("y3"), var("x3")
            ));
            assertEquals(correctUnifier, unifier);
            assertEquals(MultiUnifierImpl.nonExistent(), unifier2);
        }
    }

    @Test
    public void testUnification_RULE_TernaryRelation_ParentRepeatsMetaRoles(){
        try( TransactionImpl tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{(role: $x, role: $y, role2: $z) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role3: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role2: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $v, role2: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $u, role2: $q) isa ternary;}"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            var("u"), var("x"),
                            var("v"), var("z"),
                            var("q"), var("y")),
                    ImmutableMultimap.of(
                            var("u"), var("y"),
                            var("v"), var("z"),
                            var("q"), var("x"))
            );
            assertEquals(correctUnifier, unifier);
            assertEquals(2, unifier.size());

            MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier2 = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            var("u"), var("x"),
                            var("v"), var("y"),
                            var("q"), var("z")),
                    ImmutableMultimap.of(
                            var("u"), var("x"),
                            var("v"), var("z"),
                            var("q"), var("y")),
                    ImmutableMultimap.of(
                            var("u"), var("y"),
                            var("v"), var("z"),
                            var("q"), var("x")),
                    ImmutableMultimap.of(
                            var("u"), var("y"),
                            var("v"), var("x"),
                            var("q"), var("z"))
            );
            assertEquals(correctUnifier2, unifier2);
            assertEquals(4, unifier2.size());

            MultiUnifier unifier3 = childQuery3.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier3 = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            var("u"), var("x"),
                            var("v"), var("y"),
                            var("q"), var("z")),
                    ImmutableMultimap.of(
                            var("u"), var("y"),
                            var("v"), var("x"),
                            var("q"), var("z"))
            );
            assertEquals(correctUnifier3, unifier3);
            assertEquals(2, unifier3.size());

            MultiUnifier unifier4 = childQuery4.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier4 = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("u"), var("x"),
                    var("u"), var("y"),
                    var("q"), var("z")
            ));
            assertEquals(correctUnifier4, unifier4);
        }
    }

    @Test
    public void testUnification_RULE_TernaryRelation_ParentRepeatsRoles_ParentRepeatsRPs(){
        try( TransactionImpl tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{(role1: $x, role1: $x, role2: $y) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role3: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role2: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $v, role2: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $u, role2: $q) isa ternary;}"), tx);

            MultiUnifier emptyUnifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier emptyUnifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);

            assertTrue(emptyUnifier.isEmpty());
            assertTrue(emptyUnifier2.isEmpty());

            MultiUnifier unifier = childQuery3.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("u"), var("x"),
                    var("v"), var("x"),
                    var("q"), var("y")
            ));
            assertEquals(correctUnifier, unifier);

            MultiUnifier unifier2 = childQuery4.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier2 = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("u"), var("x"),
                    var("q"), var("y")
            ));
            assertEquals(correctUnifier2, unifier2);
        }
    }

    @Test
    public void testUnification_RULE_TernaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes(){
        try( TransactionImpl tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{$x1 isa threeRoleEntity;$x2 isa threeRoleEntity2; $x3 isa threeRoleEntity3;($x1, $x2, $x3) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{$y3 isa threeRoleEntity3;$y2 isa threeRoleEntity2;$y1 isa threeRoleEntity;($y2, $y3, $y1) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{$y3 isa threeRoleEntity3;$y2 isa threeRoleEntity2;$y1 isa threeRoleEntity;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("y1"), var("x1"),
                    var("y2"), var("x2"),
                    var("y3"), var("x3")
            ));
            assertEquals(correctUnifier, unifier);
            assertEquals(correctUnifier, unifier2);
        }
    }

    @Test // subSubThreeRoleEntity sub subThreeRoleEntity sub threeRoleEntity3
    public void testUnification_RULE_TernaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes_TypeHierarchyInvolved(){
        try( TransactionImpl tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{$x1 isa threeRoleEntity;$x2 isa subThreeRoleEntity; $x3 isa subSubThreeRoleEntity;($x1, $x2, $x3) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{$y1 isa threeRoleEntity;$y2 isa subThreeRoleEntity;$y3 isa subSubThreeRoleEntity;($y2, $y3, $y1) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{$y1 isa threeRoleEntity;$y2 isa subThreeRoleEntity;$y3 isa subSubThreeRoleEntity;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            var("y1"), var("x1"),
                            var("y2"), var("x2"),
                            var("y3"), var("x3")),
                    ImmutableMultimap.of(
                            var("y1"), var("x1"),
                            var("y2"), var("x3"),
                            var("y3"), var("x2")),
                    ImmutableMultimap.of(
                            var("y1"), var("x2"),
                            var("y2"), var("x1"),
                            var("y3"), var("x3")),
                    ImmutableMultimap.of(
                            var("y1"), var("x2"),
                            var("y2"), var("x3"),
                            var("y3"), var("x1")),
                    ImmutableMultimap.of(
                            var("y1"), var("x3"),
                            var("y2"), var("x1"),
                            var("y3"), var("x2")),
                    ImmutableMultimap.of(
                            var("y1"), var("x3"),
                            var("y2"), var("x2"),
                            var("y3"), var("x1"))
            );
            assertEquals(correctUnifier, unifier);
            assertEquals(correctUnifier, unifier2);
        }
    }


    @Test
    public void testUnification_RULE_ResourcesWithTypes(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            String parentQuery = "{$x has resource $r; $x isa baseRoleEntity;}";

            String childQuery = "{$r has resource $x; $r isa subRoleEntity;}";
            String childQuery2 = "{$x1 has resource $x; $x1 isa subSubRoleEntity;}";
            String baseQuery = "{$r has resource $x; $r isa entity;}";

            unificationWithResultChecks(parentQuery, childQuery, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentQuery, childQuery2, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentQuery, baseQuery, true, true, true, UnifierType.RULE, tx);
        }
    }

    @Test
    public void testUnification_RULE_BinaryRelationWithRoleAndTypeHierarchy_MetaTypeParent(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            String parentRelation = "{(baseRole1: $x, baseRole2: $y); $x isa entity; $y isa entity;}";

            String specialisedRelation = "{(subRole1: $u, anotherSubRole2: $v); $u isa baseRoleEntity; $v isa baseRoleEntity;}";
            String specialisedRelation2 = "{(subRole1: $y, anotherSubRole2: $x); $y isa baseRoleEntity; $x isa baseRoleEntity;}";
            String specialisedRelation3 = "{(subRole1: $u, anotherSubRole2: $v); $u isa subRoleEntity; $v isa subRoleEntity;}";
            String specialisedRelation4 = "{(subRole1: $y, anotherSubRole2: $x); $y isa subRoleEntity; $x isa subRoleEntity;}";
            String specialisedRelation5 = "{(subSubRole1: $u, subSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity;}";
            String specialisedRelation6 = "{(subSubRole1: $y, subSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity;}";

            unificationWithResultChecks(parentRelation, specialisedRelation, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation2, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation3, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation4, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation5, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation6, false, false, true, UnifierType.RULE, tx);
        }
    }

    @Test
    public void testUnification_RULE_BinaryRelationWithRoleAndTypeHierarchy_BaseRoleParent(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            String baseParentRelation = "{(baseRole1: $x, baseRole2: $y); $x isa baseRoleEntity; $y isa baseRoleEntity;}";
            String parentRelation = "{(baseRole1: $x, baseRole2: $y); $x isa subSubRoleEntity; $y isa subSubRoleEntity;}";

            String specialisedRelation = "{(subRole1: $u, anotherSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity;}";
            String specialisedRelation2 = "{(subRole1: $y, anotherSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity;}";
            String specialisedRelation3 = "{(subSubRole1: $u, subSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity;}";
            String specialisedRelation4 = "{(subSubRole1: $y, subSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity;}";

            unificationWithResultChecks(baseParentRelation, specialisedRelation, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(baseParentRelation, specialisedRelation2, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(baseParentRelation, specialisedRelation3, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(baseParentRelation, specialisedRelation4, false, false, true, UnifierType.RULE, tx);

            unificationWithResultChecks(parentRelation, specialisedRelation, false, false, false, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation2, false, false, false, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation3, false, false, false, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation4, false, false, false, UnifierType.RULE, tx);
        }
    }

    @Test
    public void testUnification_RULE_BinaryRelationWithRoleAndTypeHierarchy_BaseRoleParent_middleTypes(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            String parentRelation = "{(baseRole1: $x, baseRole2: $y); $x isa subRoleEntity; $y isa subRoleEntity;}";

            String specialisedRelation = "{(subRole1: $u, anotherSubRole2: $v); $u isa subRoleEntity; $v isa subSubRoleEntity;}";
            String specialisedRelation2 = "{(subRole1: $y, anotherSubRole2: $x); $y isa subRoleEntity; $x isa subSubRoleEntity;}";
            String specialisedRelation3 = "{(subSubRole1: $u, subSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity;}";
            String specialisedRelation4 = "{(subSubRole1: $y, subSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity;}";

            unificationWithResultChecks(parentRelation, specialisedRelation, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation2, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation3, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation4, false, false, true, UnifierType.RULE, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariants_EXACT(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(differentRelationVariants.patterns(), differentRelationVariants.exactMatrix(), UnifierType.EXACT, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariants_STRUCTURAL(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(differentRelationVariants.patterns(), differentRelationVariants.structuralMatrix(), UnifierType.STRUCTURAL, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariants_RULE(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(differentRelationVariants.patterns(), differentRelationVariants.ruleMatrix(), UnifierType.RULE, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariantsWithMetaRoles_EXACT(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(differentRelationVariantsWithMetaRoles.patterns(), differentRelationVariantsWithMetaRoles.exactMatrix(), UnifierType.EXACT, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariantsWithMetaRoles_STRUCTURAL(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(differentRelationVariantsWithMetaRoles.patterns(), differentRelationVariantsWithMetaRoles.structuralMatrix(), UnifierType.STRUCTURAL, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariantsWithMetaRoles_RULE(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(differentRelationVariantsWithMetaRoles.patterns(), differentRelationVariantsWithMetaRoles.ruleMatrix(), UnifierType.RULE, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariantsWithRelationVariable_EXACT(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(differentRelationVariantsWithRelationVariable.patterns(), differentRelationVariantsWithRelationVariable.exactMatrix(), UnifierType.EXACT, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariantsWithRelationVariable_STRUCTURAL(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(differentRelationVariantsWithRelationVariable.patterns(), differentRelationVariantsWithRelationVariable.structuralMatrix(), UnifierType.STRUCTURAL, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariantsWithRelationVariable_RULE(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(differentRelationVariantsWithRelationVariable.patterns(), differentRelationVariantsWithRelationVariable.ruleMatrix(), UnifierType.RULE, tx);
        }
    }

    @Test
    public void testUnification_differentTypeVariants_EXACT(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            List<String> qs = differentTypeVariants.patterns();
            subListExcluding(qs, Lists.newArrayList(3, 4, 7, 8)).forEach(q -> exactUnification(q, qs, new ArrayList<>(), tx));
            exactUnification(qs.get(3), qs, Collections.singletonList(qs.get(4)), tx);
            exactUnification(qs.get(4), qs, Collections.singletonList(qs.get(3)), tx);
            exactUnification(qs.get(7), qs, subList(qs, Lists.newArrayList(8)), tx);
            exactUnification(qs.get(8), qs, subList(qs, Lists.newArrayList(7)), tx);
        }
    }

    @Test
    public void testUnification_differentTypeVariants_STRUCTURAL(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    differentTypeVariants.patterns(),
                    differentTypeVariants.patterns(),
                    differentTypeVariants.structuralMatrix(),
                    UnifierType.STRUCTURAL,
                    tx
            );
        }
    }

    @Test
    public void testUnification_differentTypeVariants_RULE(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    differentTypeVariants.patterns(),
                    differentTypeVariants.patterns(),
                    differentTypeVariants.ruleMatrix(),
                    UnifierType.RULE,
                    tx
            );
        }
    }

    @Test
    public void testUnification_differentResourceVariants_EXACT(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            List<String> qs = differentResourceVariants.patterns();

            exactUnification(qs.get(0), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(1), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(2), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(3), qs, new ArrayList<>(), tx);

            exactUnification(qs.get(4), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(5), qs, new ArrayList<>(), tx);

            exactUnification(qs.get(6), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(7), qs, new ArrayList<>(), tx);

            exactUnification(qs.get(8), qs, Collections.singletonList(qs.get(10)), tx);
            exactUnification(qs.get(9), qs, Collections.singletonList(qs.get(11)), tx);
            exactUnification(qs.get(10), qs, Collections.singletonList(qs.get(8)), tx);
            exactUnification(qs.get(11), qs, Collections.singletonList(qs.get(9)), tx);

            exactUnification(qs.get(12), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(13), qs, new ArrayList<>(), tx);

            exactUnification(qs.get(14), qs, Collections.singletonList(qs.get(16)), tx);
            exactUnification(qs.get(15), qs, Collections.singletonList(qs.get(17)), tx);
            exactUnification(qs.get(16), qs, Collections.singletonList(qs.get(14)), tx);
            exactUnification(qs.get(17), qs, Collections.singletonList(qs.get(15)), tx);

            exactUnification(qs.get(18), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(19), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(20), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(21), qs, new ArrayList<>(), tx);

            exactUnification(qs.get(22), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(23), qs, Collections.singletonList(qs.get(24)), tx);
            exactUnification(qs.get(24), qs, Collections.singletonList(qs.get(23)), tx);

            exactUnification(qs.get(25), qs, subList(qs, Lists.newArrayList(26)), tx);
            exactUnification(qs.get(26), qs, subList(qs, Lists.newArrayList(25)), tx);

            exactUnification(qs.get(27), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(28), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(29), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(30), qs, new ArrayList<>(), tx);
        }
    }

    @Test
    public void testUnification_differentResourceVariants_STRUCTURAL(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    differentResourceVariants.patterns(),
                    differentResourceVariants.patterns(),
                    differentResourceVariants.structuralMatrix(),
                    UnifierType.STRUCTURAL,
                    tx);
        }
    }

    @Test
    public void testUnification_differentResourceVariants_RULE(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    differentResourceVariants.patterns(),
                    differentResourceVariants.patterns(),
                    differentResourceVariants.ruleMatrix(),
                    UnifierType.RULE,
                    tx);
        }
    }

    @Test
    public void testUnification_orthogonalityOfVariants_EXACT(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            List<List<String>> queryTypes = Lists.newArrayList(
                    differentRelationVariants.patterns(),
                    differentRelationVariantsWithRelationVariable.patterns(),
                    differentTypeVariants.patterns(),
                    differentResourceVariants.patterns()
            );
            queryTypes.forEach(qt -> subListExcludingElements(queryTypes, Collections.singletonList(qt)).forEach(qto -> qt.forEach(q -> exactUnification(q, qto, new ArrayList<>(), tx))));
        }
    }

    @Test
    public void testUnification_orthogonalityOfVariants_STRUCTURAL(){
        try( TransactionImpl tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            List<List<String>> queryTypes = Lists.newArrayList(
                    differentRelationVariants.patterns(),
                    differentRelationVariantsWithRelationVariable.patterns(),
                    differentTypeVariants.patterns(),
                    differentResourceVariants.patterns()
            );
            queryTypes.forEach(qt -> subListExcludingElements(queryTypes, Collections.singletonList(qt)).forEach(qto -> qt.forEach(q -> structuralUnification(q, qto, new ArrayList<>(), tx))));
        }
    }


    private void unification(String child, List<String> queries, List<String> queriesWithUnifier, UnifierType unifierType, TransactionImpl tx){
        queries.forEach(parent -> unification(child, parent, queriesWithUnifier.contains(parent) || parent.equals(child), unifierType, tx));
    }

    private void unification(List<String> queries, int[][] resultMatrix, UnifierType unifierType, TransactionImpl tx){
        unification(queries, queries, resultMatrix, unifierType, tx);
    }

    private void unification(List<String> children, List<String> parents, int[][] resultMatrix, UnifierType unifierType, TransactionImpl tx){
        int i = 0;
        int j = 0;
        for (String child : children) {
            for (String parent : parents) {
                unification(child, parent, resultMatrix[i][j] == 1, unifierType, tx);
                j++;
            }
            i++;
            j = 0;
        }
    }

    private void structuralUnification(String child, List<String> queries, List<String> queriesWithUnifier, TransactionImpl tx){
        unification(child, queries, queriesWithUnifier, UnifierType.STRUCTURAL, tx);
    }

    private void exactUnification(String child, List<String> queries, List<String> queriesWithUnifier, TransactionImpl tx){
        unification(child, queries, queriesWithUnifier, UnifierType.EXACT, tx);
    }

    private void ruleUnification(String child, List<String> queries, List<String> queriesWithUnifier, TransactionImpl tx){
        unification(child, queries, queriesWithUnifier, UnifierType.RULE, tx);
    }

    private MultiUnifier unification(String childString, String parentString, boolean unifierExists, UnifierType unifierType, TransactionImpl tx){
        ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childString), tx);
        ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentString), tx);

        if (unifierType.equivalence() != null) queryEquivalence(child, parent, unifierExists, unifierType.equivalence());
        MultiUnifier multiUnifier = child.getMultiUnifier(parent, unifierType);
        assertEquals("Unexpected unifier: " + multiUnifier + " between the child - parent pair:\n" + child + " :\n" + parent, unifierExists, !multiUnifier.isEmpty());
        if (unifierExists && unifierType != UnifierType.RULE){
            MultiUnifier multiUnifierInverse = parent.getMultiUnifier(child, unifierType);
            assertEquals("Unexpected unifier inverse: " + multiUnifier + " between the child - parent pair:\n" + parent + " :\n" + child, unifierExists, !multiUnifierInverse.isEmpty());
            assertEquals(multiUnifierInverse, multiUnifier.inverse());
        }
        return multiUnifier;
    }

    /**
     * checks the correctness and uniqueness of an EXACT unifier required to unify child query with parent
     * @param parentString parent query string
     * @param childString child query string
     * @param checkInverse flag specifying whether the inverse equality u^{-1}=u(parent, child) of the unifier u(child, parent) should be checked
     * @param ignoreTypes flag specifying whether the types should be disregarded and only role players checked for containment
     * @param checkEquality if true the parent and child answers will be checked for equality, otherwise they are checked for containment of child answers in parent
     */
    private void unificationWithResultChecks(String parentString, String childString, boolean checkInverse, boolean checkEquality, boolean ignoreTypes, UnifierType unifierType, TransactionImpl<?> tx){
        ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childString), tx);
        ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentString), tx);
        Unifier unifier = unification(childString, parentString, true, unifierType, tx).getUnifier();

        List<ConceptMap> childAnswers = child.getQuery().execute();
        List<ConceptMap> unifiedAnswers = childAnswers.stream()
                .map(a -> a.unify(unifier))
                .filter(a -> !a.isEmpty())
                .collect(Collectors.toList());
        List<ConceptMap> parentAnswers = parent.getQuery().execute();

        if (checkInverse) {
            Unifier inverse = parent.getMultiUnifier(child, unifierType).getUnifier();
            assertEquals(unifier.inverse(), inverse);
            assertEquals(unifier, inverse.inverse());
        }

        assertTrue(!childAnswers.isEmpty());
        assertTrue(!unifiedAnswers.isEmpty());
        assertTrue(!parentAnswers.isEmpty());

        Set<Var> parentNonTypeVariables = Sets.difference(parent.getAtom().getVarNames(), Sets.newHashSet(parent.getAtom().getPredicateVariable()));
        if (!checkEquality){
            if(!ignoreTypes){
                assertTrue(parentAnswers.containsAll(unifiedAnswers));
            } else {
                List<ConceptMap> projectedParentAnswers = parentAnswers.stream().map(ans -> ans.project(parentNonTypeVariables)).collect(Collectors.toList());
                List<ConceptMap> projectedUnified = unifiedAnswers.stream().map(ans -> ans.project(parentNonTypeVariables)).collect(Collectors.toList());
                assertTrue(projectedParentAnswers.containsAll(projectedUnified));
            }

        } else {
            Unifier inverse = unifier.inverse();
            if(!ignoreTypes) {
                assertCollectionsEqual(parentAnswers, unifiedAnswers);
                List<ConceptMap> parentToChild = parentAnswers.stream().map(a -> a.unify(inverse)).collect(Collectors.toList());
                assertCollectionsEqual(parentToChild, childAnswers);
            } else {
                Set<Var> childNonTypeVariables = Sets.difference(child.getAtom().getVarNames(), Sets.newHashSet(child.getAtom().getPredicateVariable()));
                List<ConceptMap> projectedParentAnswers = parentAnswers.stream().map(ans -> ans.project(parentNonTypeVariables)).collect(Collectors.toList());
                List<ConceptMap> projectedUnified = unifiedAnswers.stream().map(ans -> ans.project(parentNonTypeVariables)).collect(Collectors.toList());
                List<ConceptMap> projectedChild = childAnswers.stream().map(ans -> ans.project(childNonTypeVariables)).collect(Collectors.toList());

                assertCollectionsEqual(projectedParentAnswers, projectedUnified);
                List<ConceptMap> projectedParentToChild = projectedParentAnswers.stream()
                        .map(a -> a.unify(inverse))
                        .map(ans -> ans.project(childNonTypeVariables))
                        .collect(Collectors.toList());
                assertCollectionsEqual(projectedParentToChild, projectedChild);
            }
        }
    }

    private static <T> void assertCollectionsEqual(Collection<T> c1, Collection<T> c2) {
        assertTrue(isEqualCollection(c1, c2));
    }

    private void queryEquivalence(ReasonerAtomicQuery a, ReasonerAtomicQuery b, boolean queryExpectation, ReasonerQueryEquivalence equiv){
        singleQueryEquivalence(a, a, true, equiv);
        singleQueryEquivalence(b, b, true, equiv);
        singleQueryEquivalence(a, b, queryExpectation, equiv);
        singleQueryEquivalence(b, a, queryExpectation, equiv);
    }

    private void singleQueryEquivalence(ReasonerAtomicQuery a, ReasonerAtomicQuery b, boolean queryExpectation, ReasonerQueryEquivalence equiv){
        assertEquals(equiv.name() + " - Queries:\n" + a.toString() + "\n=?\n" + b.toString(), queryExpectation, equiv.equivalent(a, b));

        //check hash additionally if need to be equal
        if (queryExpectation) {
            assertEquals(a.toString() + " hash=? " + b.toString(), equiv.hash(a), equiv.hash(b));
        }
    }

    private Concept getConceptByResourceValue(TransactionImpl<?> tx, String id){
        Set<Concept> instances = tx.getAttributesByValue(id)
                .stream().flatMap(Attribute::owners).collect(Collectors.toSet());
        if (instances.size() != 1)
            throw new IllegalStateException("Something wrong, multiple instances with given res value");
        return instances.iterator().next();
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString){
        Set<VarPatternAdmin> vars = Graql.parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}