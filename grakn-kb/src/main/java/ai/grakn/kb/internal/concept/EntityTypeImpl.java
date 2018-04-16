/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.kb.internal.concept;

/*-
 * #%L
 * grakn-kb
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.kb.internal.structure.VertexElement;
import ai.grakn.util.Schema;

/**
 * <p>
 *     {@link SchemaConcept} used to represent categories.
 * </p>
 *
 * <p>
 *     A {@link SchemaConcept} which represents categories instances can fall within.
 *     Any instance of a {@link EntityType} is called an {@link Entity}.
 * </p>
 *
 * @author fppt
 *
 */
public class EntityTypeImpl extends TypeImpl<EntityType, Entity> implements EntityType{
    private EntityTypeImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    private EntityTypeImpl(VertexElement vertexElement, EntityType type) {
        super(vertexElement, type);
    }

    public static EntityTypeImpl get(VertexElement vertexElement){
        return new EntityTypeImpl(vertexElement);
    }

    public static EntityTypeImpl create(VertexElement vertexElement, EntityType type){
        return new EntityTypeImpl(vertexElement, type);
    }

    @Override
    public Entity addEntity() {
        return addInstance(Schema.BaseType.ENTITY, (vertex, type) -> vertex().tx().factory().buildEntity(vertex, type), false, true);
    }

    public Entity addEntityInferred() {
        return addInstance(Schema.BaseType.ENTITY, (vertex, type) -> vertex().tx().factory().buildEntity(vertex, type), true, true);
    }

    public static EntityTypeImpl from(EntityType entityType){
        return (EntityTypeImpl) entityType;
    }
}
