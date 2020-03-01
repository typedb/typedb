/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graph.graphdb.types;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.schema.Parameter;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public enum ParameterType {

    MAPPING("mapping"),

    INDEX_POSITION("index-pos"),

    MAPPED_NAME("mapped-name"),

    STATUS("status"),

    /** Maximum number of levels to be used in the spatial prefix tree where applicable. **/
    INDEX_GEO_MAX_LEVELS("index-geo-max-levels"),

    /** Distance error percent used to determine precision in spatial prefix tree where applicable. **/
    INDEX_GEO_DIST_ERROR_PCT("index-geo-dist-error-pct"),
    
    /** Analyzer for String Type with mapping STRING**/
    STRING_ANALYZER("string-analyzer"),

    /** Analyzer for String Type with mapping TEXT**/
    TEXT_ANALYZER("text-analyzer"),
    ;

    private static final String CUSTOM_PARAMETER_PREFIX = "%`custom%`";

    private final String name;

    ParameterType(String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name=name;
    }

    @Override
    public String toString() {
        return name;
    }

    public String getName() {
        return name;
    }

    public<V> V findParameter(Parameter[] parameters, V defaultValue) {
        V result = null;
        for (Parameter p : parameters) {
            if (p.key().equalsIgnoreCase(name)) {
                Object value = p.value();
                Preconditions.checkNotNull(value, "Invalid mapping specified: %s",value);
                Preconditions.checkArgument(result==null,"Multiple mappings specified");
                result = (V)value;
            }
        }
        if (result==null) return defaultValue;
        return result;
    }

    public boolean hasParameter(Parameter[] parameters) {
        return findParameter(parameters,null)!=null;
    }

    public<V> Parameter<V> getParameter(V value) {
        return new Parameter<>(name, value);
    }

    public static String customParameterName(String name){
        return CUSTOM_PARAMETER_PREFIX + name;
    }

    public static List<Parameter> getCustomParameters(Parameter[] parameters){

        return Arrays.stream(parameters)
            .filter(p -> p.key().startsWith(CUSTOM_PARAMETER_PREFIX))
            .map(p -> new Parameter<>(p.key().substring(CUSTOM_PARAMETER_PREFIX.length()), p.value()))
            .collect(Collectors.toList());
    }

}
