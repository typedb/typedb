package ai.grakn.graql.internal.reasoner.utils.conversion;

import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import com.google.common.collect.Multimap;

/**
 * <p>
 * TypeConverter interface for conversion between compatible types.
 * </p>
 *
 * @author Kasper Piskorski
 */
public interface TypeConverter<T extends Type>{
    /**
     * convert a given type to a map of relation types in which it can play roles
     * and the corresponding role types including entity type hierarchy
     * @param type to be converted
     * @return map of relation types in which it can play roles and the corresponding role types
     */
    Multimap<RelationType, RoleType> toRelationMultimap(T type);
}

