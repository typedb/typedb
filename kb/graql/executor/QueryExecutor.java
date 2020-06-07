/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.kb.graql.executor;

import grakn.core.concept.answer.AnswerGroup;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.answer.Void;
import grakn.core.kb.graql.planning.gremlin.GraqlTraversal;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlUndefine;
import graql.lang.query.MatchClause;

import java.util.stream.Stream;

public interface QueryExecutor {
    Stream<ConceptMap> match(MatchClause matchClause);

    Stream<ConceptMap> define(GraqlDefine query);

    Stream<ConceptMap> undefine(GraqlUndefine query);

    Stream<ConceptMap> insert(GraqlInsert query);

    Void delete(GraqlDelete query);

    Stream<ConceptMap> get(GraqlGet query);

    Stream<Numeric> aggregate(GraqlGet.Aggregate query);

    Stream<AnswerGroup<ConceptMap>> get(GraqlGet.Group query);

    Stream<AnswerGroup<Numeric>> get(GraqlGet.Group.Aggregate query);
}
