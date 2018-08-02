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

package ai.grakn.graql;

import ai.grakn.GraknTx;
import ai.grakn.graql.answer.ConceptMap;

import javax.annotation.CheckReturnValue;
import java.util.List;
import java.util.Set;

/**
 * a query used for finding data in a knowledge base that matches the given patterns.
 * <p>
 * The {@link GetQuery} is a pattern-matching query. The patterns are described in a declarative fashion,
 * then the {@link GetQuery} will traverse the knowledge base in an efficient fashion to find any matching answers.
 * <p>
 * @see ConceptMap
 *
 * @author Felix Chapman
 */
public interface GetQuery extends Query<List<ConceptMap>>, Streamable<ConceptMap> {

    /**
     * @param tx the transaction to execute the query on
     * @return a new {@link GetQuery} with the transaction set
     */
    @Override
    GetQuery withTx(GraknTx tx);

    /**
     * Get the {@link Match} this {@link GetQuery} contains
     */
    @CheckReturnValue
    Match match();

    /**
     * Get the {@link Var}s this {@link GetQuery} will select from the answers
     */
    @CheckReturnValue
    Set<Var> vars();
}
