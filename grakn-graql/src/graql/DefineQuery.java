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

package grakn.core.graql;

import grakn.core.concept.SchemaConcept;
import grakn.core.graql.answer.ConceptMap;

import java.util.Collection;

/**
 * A query for defining {@link SchemaConcept}s.
 * <p>
 *     The query will define all {@link SchemaConcept}s described in the {@link VarPattern}s provided and return an
 *     {@link ConceptMap} containing bindings for all {@link Var}s in the {@link VarPattern}s.
 * </p>
 *
 */
public interface DefineQuery extends Query<ConceptMap> {

    /**
     * Get the {@link VarPattern}s describing what {@link SchemaConcept}s to define.
     */
    Collection<? extends VarPattern> varPatterns();
}
