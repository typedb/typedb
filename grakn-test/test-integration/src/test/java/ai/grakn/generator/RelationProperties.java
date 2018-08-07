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

import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.internal.pattern.property.RelationshipProperty;
import com.google.common.collect.ImmutableMultiset;

/**
 * @author Felix Chapman
 */
public class RelationProperties extends AbstractGenerator<RelationshipProperty> {

    public RelationProperties() {
        super(RelationshipProperty.class);
    }

    @Override
    public RelationshipProperty generate() {
        return RelationshipProperty.of(ImmutableMultiset.copyOf(listOf(RelationPlayer.class, 1, 3)));
    }
}