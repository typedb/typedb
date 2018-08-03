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

package ai.grakn.generator;

import ai.grakn.concept.Label;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;

import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.toImmutableSet;

/**
 * Generator that generates meta type names only
 *
 * @author Felix Chapman
 */
public class MetaLabels extends AbstractGenerator<Label> {

    private static final ImmutableSet<Label> META_TYPE_LABELS =
            Stream.of(Schema.MetaSchema.values()).map(Schema.MetaSchema::getLabel).collect(toImmutableSet());

    public MetaLabels() {
        super(Label.class);
    }

    @Override
    public Label generate() {
        return random.choose(META_TYPE_LABELS);
    }
}
