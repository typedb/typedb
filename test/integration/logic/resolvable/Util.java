/*
 * Copyright (C) 2021 Grakn Labs
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


package grakn.core.logic.resolvable;

import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.traversal.common.Identifier;
import graql.lang.Graql;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Util {


    public static Conjunction resolvedConjunction(String query, LogicManager logicMgr) {
        Conjunction conjunction = Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
        logicMgr.typeResolver().resolvePositive(conjunction);
        return conjunction;
    }


    public static Rule createRule(String label, String whenConjunctionPattern, String thenThingPattern, LogicManager logicMgr) {
        Rule rule = logicMgr.putRule(label, Graql.parsePattern(whenConjunctionPattern).asConjunction(),
                                     Graql.parseVariable(thenThingPattern).asThing());
        return rule;
    }

    public static Map<String, Set<String>> getStringMapping(Map<Identifier.Variable.Retrievable, Set<Identifier.Variable>> map) {
        return map.entrySet().stream().collect(Collectors.toMap(v -> v.getKey().toString(),
                                                                e -> e.getValue().stream().map(Identifier::toString).collect(Collectors.toSet()))
        );
    }

}
