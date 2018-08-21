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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.test.kbs;

import ai.grakn.GraknTx;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Thing;
import ai.grakn.test.rule.SampleKBContext;

import java.util.Set;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toSet;

/**
 * Base for all test graphs.
 * @author borislav
 *
 */
public abstract class TestKB {

    protected void buildSchema(GraknTx tx){};

    protected void buildInstances(GraknTx tx){};

    protected void buildRelations(){};

    protected void buildRules(GraknTx tx){};

    public Consumer<GraknTx> build() {
        return (GraknTx tx) -> {
            buildSchema(tx);
            buildInstances(tx);
            buildRelations();
            buildRules(tx);
        };
    }

    public SampleKBContext makeContext() {
        return SampleKBContext.load(build());
    }

    public static Thing putEntityWithResource(GraknTx tx, String id, EntityType type, Label key) {
        Thing inst = type.create();
        putResource(inst, tx.getSchemaConcept(key), id);
        return inst;
    }

    public static <T> void putResource(Thing thing, AttributeType<T> attributeType, T resource) {
        Attribute attributeInstance = attributeType.create(resource);
        thing.has(attributeInstance);
    }

    public static Thing getInstance(GraknTx tx, String id){
        Set<Thing> things = tx.getAttributesByValue(id)
                .stream().flatMap(Attribute::owners).collect(toSet());
        if (things.size() != 1) {
            throw new IllegalStateException("Multiple things with given resource value");
        }
        return things.iterator().next();
    }
}
