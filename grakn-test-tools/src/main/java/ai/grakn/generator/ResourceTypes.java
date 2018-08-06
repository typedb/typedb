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

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Label;

/**
 * A generator that produces {@link AttributeType}s
 *
 * @author Felix Chapman
 */
public class ResourceTypes extends AbstractTypeGenerator<AttributeType> {

    public ResourceTypes() {
        super(AttributeType.class);
    }

    @Override
    protected AttributeType newSchemaConcept(Label label) {
        AttributeType.DataType<?> dataType = gen(AttributeType.DataType.class);

        return tx().putAttributeType(label, dataType);
    }

    @Override
    protected AttributeType metaSchemaConcept() {
        return tx().admin().getMetaAttributeType();
    }
}
