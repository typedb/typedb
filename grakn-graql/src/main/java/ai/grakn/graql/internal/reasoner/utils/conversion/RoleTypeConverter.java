package ai.grakn.graql.internal.reasoner.utils.conversion;

import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;

/**
 * <p>
 * Implementation of {@link TypeConverter} allowing for conversion of role types to compatible types.
 * </p>
 *
 * @author Kasper Piskorski
 */
public class RoleTypeConverter implements TypeConverter<RoleType> {
    @Override
    public Multimap<RelationType, RoleType> toRelationMultimap(RoleType role) {
        Multimap<RelationType, RoleType> relationMap = HashMultimap.create();
        Collection<RoleType> roleTypes = role.subTypes();
        roleTypes
                .forEach(roleType -> {
                    roleType.relationTypes().stream()
                            .filter(rel -> !rel.isImplicit())
                            .forEach(rel -> relationMap.put(rel, roleType));
                });
        return relationMap;
    }
}
