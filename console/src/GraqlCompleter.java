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

package ai.grakn.core.console;

import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Label;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.Autocomplete;
import ai.grakn.kb.admin.GraknAdmin;
import com.google.common.collect.ImmutableSet;
import jline.console.completer.Completer;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.toImmutableSet;

/**
 * An autocompleter for Graql.
 * Provides a default 'complete' method that will filter results to only those that pass the Graql lexer
 *
 * @author Felix Chapman
 */
public class GraqlCompleter implements Completer {

    private final ImmutableSet<Label> labels;

    private GraqlCompleter(ImmutableSet<Label> labels) {
        this.labels = labels;
    }

    public static GraqlCompleter create(GraknSession session) {
        ImmutableSet<Label> labels;
        try (GraknAdmin tx = session.transaction(GraknTxType.READ).admin()) {

            Stream<SchemaConcept> metaConcepts =
                    Stream.of(tx.getMetaConcept(), tx.getMetaRole(), tx.getMetaRule()).flatMap(SchemaConcept::subs);

            labels = metaConcepts.map(SchemaConcept::label).collect(toImmutableSet());
        }
        return new GraqlCompleter(labels);
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates) {
        Set<String> labelValues = labels.stream().map(Label::getValue).collect(toImmutableSet());
        Autocomplete autocomplete = Autocomplete.create(labelValues, buffer, cursor);
        candidates.addAll(autocomplete.getCandidates());
        return autocomplete.getCursorPosition();
    }
}
