/*
 * Copyright (C) 2021 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */


package com.vaticle.typedb.core.logic.resolvable;

import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.TypeQL;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Util {

    public static Conjunction resolvedConjunction(String query, LogicManager logicMgr) {
        Disjunction disjunction = Disjunction.create(TypeQL.parsePattern(query).asConjunction().normalise());
        assert disjunction.conjunctions().size() == 1;
        logicMgr.typeResolver().resolveDisjunction(disjunction);
        return disjunction.conjunctions().get(0);
    }

    public static Rule createRule(String label, String whenConjunctionPattern, String thenPattern, LogicManager logicMgr) {
        Rule rule = logicMgr.putRule(label, TypeQL.parsePattern(whenConjunctionPattern).asConjunction(),
                                     TypeQL.parseVariable(thenPattern).asThing());
        return rule;
    }

    public static Map<String, Set<String>> getStringMapping(Map<Identifier.Variable.Retrievable, Set<Identifier.Variable>> map) {
        return map.entrySet().stream().collect(Collectors.toMap(
                v -> v.getKey().toString(),
                e -> e.getValue().stream().map(Identifier::toString).collect(Collectors.toSet())
        ));
    }

}
