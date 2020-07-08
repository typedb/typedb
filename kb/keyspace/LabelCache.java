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
 *
 */

package grakn.core.kb.keyspace;

import grakn.core.kb.concept.api.LabelId;
import graql.lang.statement.Label;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Cache labels and their corresponding LabelId (numerical identifier of a Label)
 * This allows us to bypass doing further lookups
 */
public class LabelCache implements LabelCacheReader {

    // complete labels to label IDs - except for roles, these will be unscoped
    // "complete" means there is exactly 1 LabelID corresponding to the Label, and that scope is present when it should be
    Map<Label, LabelId> labelIds;
    // mapping from an unscoped label to all possible scopes
    Map<String, Set<String>> scopes;

    public LabelCache() {
        labelIds = new ConcurrentHashMap<>();
        scopes = new ConcurrentHashMap<>();
    }

    public LabelCache(LabelCache toCopy) {
        this();
        this.absorb(toCopy);
    }

    public boolean isCompleteLabelCached(Label completeLabel) {
        return labelIds.containsKey(completeLabel);
    }

    public LabelId completeLabelToId(Label completeLabel) {
        return labelIds.get(completeLabel);
    }

    public Stream<LabelId> ambiguousLabelToId(Label unscopedLabel) {
        if (unscopedLabel.scope() != null) {
            // TODO better exception
            throw new RuntimeException("Only unscoped labels can be ambiguous");
        }
        String unscoped = unscopedLabel.name();
        return scopes.get(unscoped)
                .stream()
                .map(scope -> labelIds.get(Label.of(scope, unscoped)))
                .filter(Objects::nonNull);
    }

    public void cacheCompleteLabel(Label completeLabel, LabelId labelId) {
        labelIds.put(completeLabel, labelId);
        String labelScope = completeLabel.scope();
        if (labelScope != null) {
            scopes.putIfAbsent(completeLabel.name(), new HashSet<>());
            scopes.get(completeLabel.name()).add(labelScope);
        }
    }

    public boolean isEmpty() {
        return labelIds.isEmpty();
    }

    public void clear() {
        scopes.clear();
        labelIds.clear();
    }

    public void absorb(LabelCache toMerge) {
        labelIds.putAll(toMerge.labelIds);
        scopes.putAll(toMerge.scopes);
    }

    /**
     * remove a complete label from the cache
     * Complete means that it will contain the scoping when it is required
     * Or, in other words, there is exactly one LabelID corresponding to the Label provided
     */
    public void removeCompleteLabel(Label completeLabel) {
        labelIds.remove(completeLabel);
        String labelScope = completeLabel.scope();
        if (labelScope != null && scopes.containsKey(completeLabel.scope())) {
            scopes.get(labelScope).remove(completeLabel.name());
        }
    }

}
