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

package grakn.core.graph.diskstorage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This enum is only intended for use by JanusGraph internals.
 * It is subject to backwards-incompatible change.
 */
public enum StandardIndexProvider {
    LUCENE("grakn.core.graph.diskstorage.lucene.LuceneIndex", "lucene"),
    ELASTICSEARCH("grakn.core.graph.diskstorage.es.ElasticSearchIndex", ImmutableList.of("elasticsearch", "es")),
    SOLR("grakn.core.graph.diskstorage.solr.SolrIndex", "solr");

    private final String providerName;
    private final ImmutableList<String> shorthands;

    StandardIndexProvider(String providerName, String shorthand) {
        this(providerName, ImmutableList.of(shorthand));
    }

    StandardIndexProvider(String providerName, ImmutableList<String> shorthands) {
        this.providerName = providerName;
        this.shorthands = shorthands;
    }

    private static final ImmutableList<String> ALL_SHORTHANDS;
    private static final ImmutableMap<String, String> ALL_MANAGER_CLASSES;

    private List<String> getShorthands() {
        return shorthands;
    }

    private String getProviderName() {
        return providerName;
    }

    static {
        StandardIndexProvider[] backends = values();
        List<String> tempShorthands = new ArrayList<>();
        Map<String, String> tempClassMap = new HashMap<>();
        for (StandardIndexProvider backend : backends) {
            tempShorthands.addAll(backend.getShorthands());
            for (String shorthand : backend.getShorthands()) {
                tempClassMap.put(shorthand, backend.getProviderName());
            }
        }
        ALL_SHORTHANDS = ImmutableList.copyOf(tempShorthands);
        ALL_MANAGER_CLASSES = ImmutableMap.copyOf(tempClassMap);
    }

    public static List<String> getAllShorthands() {
        return ALL_SHORTHANDS;
    }

    public static Map<String, String> getAllProviderClasses() {
        return ALL_MANAGER_CLASSES;
    }
}
