package grakn.core.graql.reasoner.atom.materialise;

import com.google.common.collect.ImmutableMap;
import grakn.core.graql.reasoner.atom.Atom;
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

    static AtomMaterialiser create(Class type, ReasonerQueryFactory queryFactory, QueryCache queryCache){
        BiFunction<ReasonerQueryFactory, QueryCache, AtomMaterialiser> match = materialisers.get(type);
        if (match == null) throw new IllegalArgumentException();
        return match.apply(queryFactory, queryCache);
    }
}
