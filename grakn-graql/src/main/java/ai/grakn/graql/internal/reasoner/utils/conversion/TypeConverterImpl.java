package ai.grakn.graql.internal.reasoner.utils.conversion;

import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;

/**
 * <p>
 * Basic {@link TypeConverter} implementation for conversion between compatible types.
 * </p>
 *
 * @author Kasper Piskorski
 */
public class TypeConverterImpl implements TypeConverter<Type> {

    @Override
    public Multimap<RelationType, RoleType> toRelationMultimap(Type type) {
        Multimap<RelationType, RoleType> relationMap = HashMultimap.create();
        Collection<? extends Type> types = type.subTypes();
        types.stream()
                .flatMap(t -> t.plays().stream())
                .forEach(roleType -> {
                    roleType.relationTypes().stream()
                            .filter(rel -> !rel.isImplicit())
                            .forEach(rel -> relationMap.put(rel, roleType));
                });
        return relationMap;
    }
}
