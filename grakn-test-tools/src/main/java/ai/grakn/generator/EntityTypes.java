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

import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;

/**
 * Generator that produces {@link EntityType}s
 *
 * @author Felix Chapman
 */
public class EntityTypes extends AbstractTypeGenerator<EntityType> {

    public EntityTypes() {
        super(EntityType.class);
    }

    @Override
    protected EntityType newSchemaConcept(Label label) {
        return tx().putEntityType(label);
    }

    @Override
    protected EntityType metaSchemaConcept() {
        return tx().admin().getMetaEntityType();
    }
}
