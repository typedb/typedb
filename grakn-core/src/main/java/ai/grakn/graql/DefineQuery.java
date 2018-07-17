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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql;

import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.admin.ConceptMap;

import java.util.Collection;

/**
 * A query for defining {@link SchemaConcept}s.
 * <p>
 *     The query will define all {@link SchemaConcept}s described in the {@link VarPattern}s provided and return an
 *     {@link ConceptMap} containing bindings for all {@link Var}s in the {@link VarPattern}s.
 * </p>
 *
 * @author Felix Chapman
 */
public interface DefineQuery extends Query<ConceptMap> {

    /**
     * Get the {@link VarPattern}s describing what {@link SchemaConcept}s to define.
     */
    Collection<? extends VarPattern> varPatterns();
}
