/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
 *
 */

package grakn.core.graql.reasoner.operator;

import graql.lang.pattern.Pattern;
import java.util.stream.Stream;

/**
 * Interface for defining Pattern operators. The application of an operator O on an input pattern P
 * results in a pattern P' such that:
 *
 * P' = O P
 *
 */
public interface Operator {

    /**
     * exhaustive operator application
     * @param src pattern to be transformed
     * @param ctx type context for patterns
     * @return set of patterns resulting from operator application
     */
    Stream<Pattern> apply(Pattern src, TypeContext ctx);
}
