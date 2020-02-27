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

package grakn.core.graql.reasoner.atom.materialise;

import com.google.common.collect.ImmutableMap;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.kb.graql.reasoner.cache.QueryCache;
import java.util.Map;
import java.util.function.BiFunction;

public class MaterialiserFactory {

    private static final Map<Class, BiFunction<ReasonerQueryFactory, QueryCache, AtomMaterialiser>> materialisers = ImmutableMap.of(
            RelationAtom.class, RelationMaterialiser::new,
            IsaAtom.class, IsaMaterialiser::new,
            AttributeAtom.class, AttributeMaterialiser::new
    );

    public static AtomMaterialiser create(Class type, ReasonerQueryFactory queryFactory, QueryCache queryCache){
        BiFunction<ReasonerQueryFactory, QueryCache, AtomMaterialiser> match = materialisers.get(type);
        if (match == null) throw new IllegalArgumentException();
        return match.apply(queryFactory, queryCache);
    }
}
