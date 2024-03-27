/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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
        logicMgr.typeInference().applyCombination(disjunction);
        return disjunction.conjunctions().get(0);
    }

    public static Rule createRule(String label, String whenConjunctionPattern, String thenPattern, LogicManager logicMgr) {
        Rule rule = logicMgr.putRule(label, TypeQL.parsePattern(whenConjunctionPattern).asConjunction(),
                TypeQL.parseStatement(thenPattern).asThing());
        return rule;
    }

    public static Map<String, Set<String>> getStringMapping(Map<Identifier.Variable.Retrievable, Set<Identifier.Variable>> map) {
        return map.entrySet().stream().collect(Collectors.toMap(
                v -> v.getKey().toString(),
                e -> e.getValue().stream().map(Identifier::toString).collect(Collectors.toSet())
        ));
    }

}
