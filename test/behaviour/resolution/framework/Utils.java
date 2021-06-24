package com.vaticle.typedb.core.test.behaviour.resolution.framework;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class Utils {
    public static Set<Identifier.Variable.Retrievable> filterRetrievableVars(Set<Identifier.Variable> vars) {
        return iterate(vars).filter(var -> var.isName() || var.isAnonymous()).map(var -> {
            if (var.isName()) return var.asName();
            if (var.isAnonymous()) return var.asAnonymous();
            throw TypeDBException.of(ILLEGAL_STATE);
        }).toSet();
    }
}
