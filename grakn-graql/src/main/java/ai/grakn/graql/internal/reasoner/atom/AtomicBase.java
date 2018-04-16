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

package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.concept.Rule;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.Set;

/**
 *
 * <p>
 * Base {@link Atomic} implementation providing basic functionalities.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class AtomicBase implements Atomic {

    @Override public void checkValid(){}

    @Override
    public Set<String> validateAsRuleHead(Rule rule) {
        return Sets.newHashSet(ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD.getMessage(rule.getThen(), rule.getLabel()));
    }

    @Override
    public String toString(){ return getPattern().toString(); }

    boolean containsVar(Var name){ return getVarNames().contains(name);}

    public boolean isUserDefined(){ return getVarName().isUserDefinedName();}

    @Override
    public Set<Var> getVarNames(){
        Var varName = getVarName();
        return varName.isUserDefinedName()? Sets.newHashSet(varName) : Collections.emptySet();
    }

    protected Pattern createCombinedPattern(){ return getPattern();}

    @Override
    public Pattern getCombinedPattern(){
        return createCombinedPattern();
    }

    @Override
    public Atomic inferTypes(){ return inferTypes(new QueryAnswer()); }

    public Atomic inferTypes(Answer sub){ return this; }

    /**
     * @return GraknTx this atomic is defined in
     */
    protected EmbeddedGraknTx<?> tx(){
        // TODO: This cast is unsafe - ReasonerQuery should return an EmbeddedGraknTx
        return (EmbeddedGraknTx<?>) getParentQuery().tx();
    }
}

