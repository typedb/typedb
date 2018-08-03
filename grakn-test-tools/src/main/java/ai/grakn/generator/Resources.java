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

package ai.grakn.generator;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;

/**
 * A generator that produces {@link Attribute}s
<<<<<<< Updated upstream:grakn-test-tools/src/main/java/ai/grakn/generator/Resources.java
=======
 *
 * @author Felix Chapman
>>>>>>> Stashed changes:grakn-test-tools/src/main/java/ai/grakn/generator/Resources.java
 */
public class Resources extends AbstractThingGenerator<Attribute, AttributeType> {

    public Resources() {
        super(Attribute.class, ResourceTypes.class);
    }

    @Override
    protected Attribute newInstance(AttributeType type) {
        return newResource(type);
    }
}
