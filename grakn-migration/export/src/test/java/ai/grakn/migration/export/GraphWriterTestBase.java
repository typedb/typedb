/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */
package ai.grakn.migration.export;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import org.junit.After;

import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

public abstract class GraphWriterTestBase {

    protected static final GraknGraph original = Grakn.factory(Grakn.IN_MEMORY, "original").getGraph();
    protected static final GraknGraph copy = Grakn.factory(Grakn.IN_MEMORY, "copy").getGraph();
    protected static final GraphWriter writer = new GraphWriter(original);

    @After
    public void clear(){
        copy.getMetaType().instances().stream().flatMap(c -> c.asType().instances().stream()).forEach(Concept::delete);
        copy.getMetaType().instances().stream().filter(i -> {
            return !i.getId().equals("inference-rule") && !i.getId().equals("constraint-rule");
        }).forEach(Concept::delete);
    }

    public void assertDataEqual(GraknGraph one, GraknGraph two){
        one.getMetaType().instances().stream()
                .flatMap(c -> c.asType().instances().stream())
                .map(Concept::asInstance)
                .forEach(i -> assertInstanceCopied(i, two));
    }

    public void assertInstanceCopied(Instance instance, GraknGraph two){

        if(instance instanceof Entity){
            assertEntityCopied(instance.asEntity(), two);
        } else if(instance instanceof Relation){
            assertRelationCopied(instance.asRelation(), two);
        } else if(instance instanceof Rule){
            assertRuleCopied(instance.asRule(), two);
        } else if(instance instanceof Resource){
            assertResourceCopied(instance.asResource(), two);
        }

    }

    public void assertEntityCopied(Entity entity1, GraknGraph two){
        Entity entity2 = two.getEntity(entity1.getId());

        Map<String, Object> resources1 = entity1.resources().stream().collect(toMap(c -> c.type().getId(), Resource::getValue));
        Map<String, Object> resources2 = entity2.resources().stream().collect(toMap(c -> c.type().getId(), Resource::getValue));

        assertEquals(resources1, resources2);
    }

    public void assertRelationCopied(Relation relation1, GraknGraph two){
        if(relation1.rolePlayers().values().stream().anyMatch(Concept::isResource)){
            return;
        }

        RelationType relationType = two.getRelationType(relation1.type().getId());
        Map<RoleType, Instance> rolemap = relation1.rolePlayers().entrySet().stream().collect(toMap(
                e -> two.getRoleType(e.getKey().getId()),
                e -> two.getEntity(e.getValue().getId())
        ));

        assertNotNull(two.getRelation(relationType, rolemap));
    }

    public void assertResourceCopied(Resource resource1, GraknGraph two){
        assertEquals(true, two.getResourcesByValue(resource1.getValue()).stream()
                .map(Concept::type)
                .map(Type::getId)
                .anyMatch(t -> resource1.type().getId().equals(t)));
    }

    public void assertRuleCopied(Rule rule1, GraknGraph two){
        Rule rule2 = two.getRule(rule1.getId());
        assertEquals(rule1.getLHS(), rule2.getLHS());
        assertEquals(rule1.getRHS(), rule2.getRHS());
    }

    public void assertOntologiesEqual(GraknGraph one, GraknGraph two){
        boolean ontologyCorrect = one.getMetaType().instances().stream()
                .allMatch(t -> typesEqual(t.asType(), two.getType(t.getId())));
        assertEquals(true, ontologyCorrect);
    }

    public boolean typesEqual(Type one, Type two){
        System.out.println(one);
        System.out.println(two);
        return one.getId().equals(two.getId())
                && one.isAbstract().equals(two.isAbstract())
                && one.type().getId().equals(two.type().getId())
                && (!one.isResourceType() || one.asResourceType().getDataType().equals(two.asResourceType().getDataType()));
    }
}
