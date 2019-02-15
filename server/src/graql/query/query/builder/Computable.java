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
import grakn.core.graql.query.query.GraqlCompute;
import graql.lang.util.Token;

import javax.annotation.CheckReturnValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public interface Computable {

    interface Directional<T extends Computable.Directional> extends Computable{

        @CheckReturnValue
        T from(ConceptId fromID);

        @CheckReturnValue
        T to(ConceptId toID);
    }

    interface Targetable<T extends Computable.Targetable> extends Computable {

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

    interface Configurable<T extends Computable.Configurable> extends Computable {

        @CheckReturnValue
        T using(Token.Compute.Algorithm algorithm);

        @CheckReturnValue
        default T where(GraqlCompute.Argument arg, GraqlCompute.Argument... args) {
            ArrayList<GraqlCompute.Argument> argList = new ArrayList<>(args.length + 1);
            argList.add(arg);
            argList.addAll(Arrays.asList(args));

            return where(argList);
        }

        @CheckReturnValue
        T where(List<GraqlCompute.Argument> args);
    }
}
