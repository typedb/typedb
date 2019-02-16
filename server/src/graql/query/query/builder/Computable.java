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

package grakn.core.graql.query.query.builder;

import grakn.core.graql.concept.ConceptId;
import graql.lang.exception.GraqlException;
import graql.lang.util.Token;

import javax.annotation.CheckReturnValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface Computable {

    @CheckReturnValue
    Token.Compute.Method method();

    @CheckReturnValue
    Set<Token.Compute.Condition> conditionsRequired();

    @CheckReturnValue
    Optional<GraqlException> getException();

    interface Directional<T extends Computable.Directional> extends Computable {

        @CheckReturnValue
        ConceptId from();

        @CheckReturnValue
        ConceptId to();

        @CheckReturnValue
        T from(ConceptId fromID);

        @CheckReturnValue
        T to(ConceptId toID);
    }

    interface Targetable<T extends Computable.Targetable> extends Computable {

        @CheckReturnValue
        Set<String> of();

        @CheckReturnValue
        default T of(String type, String... types) {
            ArrayList<String> typeList = new ArrayList<>(types.length + 1);
            typeList.add(type);
            typeList.addAll(Arrays.asList(types));

            return of(typeList);
        }

        @CheckReturnValue
        T of(Collection<String> types);
    }

    interface Scopeable<T extends Computable.Scopeable> extends Computable {

        @CheckReturnValue
        Set<String> in();

        @CheckReturnValue
        boolean includesAttributes();

        @CheckReturnValue
        default T in(String type, String... types) {
            ArrayList<String> typeList = new ArrayList<>(types.length + 1);
            typeList.add(type);
            typeList.addAll(Arrays.asList(types));

            return in(typeList);
        }

        @CheckReturnValue
        T in(Collection<String> types);

        @CheckReturnValue
        T attributes(boolean include);
    }

    interface Configurable<T extends Computable.Configurable,
            U extends Computable.Argument, V extends Computable.Arguments> extends Computable {

        @CheckReturnValue
        Token.Compute.Algorithm using();

        @CheckReturnValue
        V where();

        @CheckReturnValue
        T using(Token.Compute.Algorithm algorithm);

        @CheckReturnValue
        @SuppressWarnings("unchecked")
        default T where(U arg, U... args) {
            ArrayList<U> argList = new ArrayList<>(args.length + 1);
            argList.add(arg);
            argList.addAll(Arrays.asList(args));

            return where(argList);
        }

        @CheckReturnValue
        T where(List<U> args);

        @CheckReturnValue
        Set<Token.Compute.Algorithm> algorithmsAccepted();

        @CheckReturnValue
        Map<Token.Compute.Algorithm, Set<Token.Compute.Param>> argumentsAccepted();

        @CheckReturnValue
        Map<Token.Compute.Algorithm, Map<Token.Compute.Param, Object>> argumentsDefault();
    }

    interface Argument<T> {

        Token.Compute.Param type();

        T value();
    }

    interface Arguments {

        @CheckReturnValue
        Optional<Long> minK();

        Optional<Long> k();

        Optional<Long> size();

        Optional<ConceptId> contains();
    }
}
