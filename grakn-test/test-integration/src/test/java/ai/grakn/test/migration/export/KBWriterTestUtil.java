/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */
package ai.grakn.test.migration.export;

import ai.grakn.GraknTx;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class KBWriterTestUtil {

    public static void insert(GraknTx graph, String query) {
        graph.graql().parse("insert " + query).execute();
    }

    public static void assertDataEqual(GraknTx one, GraknTx two){
        one.admin().getMetaConcept().subs().
                filter(Concept::isType).
                map(Concept::asType).
                flatMap(Type::instances).
                forEach(i -> assertInstanceCopied(i, two));
    }

    public static void assertInstanceCopied(Thing thing, GraknTx two){
        if(thing.isEntity()){
            assertEntityCopied(thing.asEntity(), two);
        } else if(thing.isRelationship()){
            assertRelationCopied(thing.asRelationship(), two);
        } else if(thing.isRule()){
            assertRuleCopied(thing.asRule(), two);
        } else if(thing.isAttribute()){
            assertResourceCopied(thing.asAttribute(), two);
        }
    }

    /**
     * Assert that there are the same number of entities in each graph with the same resources
     */
    public static void assertEntityCopied(Entity entity1, GraknTx two){
        Collection<Entity> entitiesFromGraph1 = entity1.attributes().flatMap(Attribute::ownerInstances).map(Concept::asEntity).collect(toSet());
        Collection<Entity> entitiesFromGraph2 = getInstancesByResources(two, entity1).stream().map(Concept::asEntity).collect(toSet());

        assertEquals(entitiesFromGraph1.size(), entitiesFromGraph2.size());
    }

    /**
     * Get all instances with the same resources
     */
    public static Collection<Thing> getInstancesByResources(GraknTx graph, Thing thing){
        return thing.attributes()
                .map(r -> getResourceFromGraph(graph, r))
                .flatMap(Attribute::ownerInstances)
                .collect(toSet());
    }

    /**
     * Get an entity that is uniquely defined by its resources
     */
    public static Thing getInstanceUniqueByResourcesFromGraph(GraknTx graph, Thing thing){
        return getInstancesByResources(graph, thing)
               .iterator().next();
    }

    public static <V> Attribute<V> getResourceFromGraph(GraknTx graph, Attribute<V> attribute){
        return (Attribute<V>) graph.getAttributeType(attribute.type().getLabel().getValue()).getAttribute(attribute.getValue());
    }

    public static void assertRelationCopied(Relationship relationship1, GraknTx two){
        if(relationship1.rolePlayers().anyMatch(Concept::isAttribute)){
            return;
        }

        RelationshipType relationshipType = two.getRelationshipType(relationship1.type().getLabel().getValue());
        Map<Role, Set<Thing>> rolemap = relationship1.allRolePlayers().entrySet().stream().collect(toMap(
                e -> two.getRole(e.getKey().getLabel().getValue()),
                e -> e.getValue().stream().
                        map(instance -> getInstanceUniqueByResourcesFromGraph(two, instance)).
                        collect(Collectors.toSet())
        ));

        boolean relationFound = relationshipType.instances().
                anyMatch(relation -> relation.allRolePlayers().equals(rolemap));

        assertTrue("The copied relation [" + relationship1 + "] was not found.", relationFound);
    }

    public static void assertResourceCopied(Attribute attribute1, GraknTx two){
        assertEquals(true, two.getAttributesByValue(attribute1.getValue()).stream()
                .map(Thing::type)
                .map(Type::getLabel)
                .anyMatch(t -> attribute1.type().getLabel().equals(t)));
    }

    public static void assertRuleCopied(Rule rule1, GraknTx two){
        Rule rule2 = getInstanceUniqueByResourcesFromGraph(two, rule1).asRule();

        assertEquals(Graql.and(rule1.getWhen()), rule2.getWhen());
        assertEquals(Graql.and(rule1.getThen()), rule2.getThen());
    }

    public static void assertOntologiesEqual(GraknTx one, GraknTx two){
        boolean schemaCorrect = one.admin().getMetaConcept().subs().filter(Concept::isType)
                .allMatch(t -> typesEqual(t.asType(), two.getSchemaConcept(t.asType().getLabel())));
        assertEquals(true, schemaCorrect);
    }

    public static boolean typesEqual(Type one, Type two){
        return one.getLabel().equals(two.getLabel())
                && one.isAbstract().equals(two.isAbstract())
                && (one.sup() == null || one.sup().getLabel().equals(two.sup().getLabel()))
                && (!one.isAttributeType() || Objects.equals(one.asAttributeType().getDataType(), two.asAttributeType().getDataType()));
    }
}
