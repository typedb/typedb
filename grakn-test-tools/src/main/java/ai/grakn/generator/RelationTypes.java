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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.generator;

import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Label;

/**
 * A generator that produces {@link RelationshipType}s
 *
 * @author Felix Chapman
 */
public class RelationTypes extends AbstractTypeGenerator<RelationshipType> {

    public RelationTypes() {
        super(RelationshipType.class);
    }

    @Override
    protected RelationshipType newSchemaConcept(Label label) {
        return tx().putRelationshipType(label);
    }

    @Override
    protected RelationshipType metaSchemaConcept() {
        return tx().admin().getMetaRelationType();
    }
}
