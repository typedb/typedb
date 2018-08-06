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

package ai.grakn.test.kbs;

import ai.grakn.GraknTx;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.SampleKBLoader;

import java.util.function.Consumer;

/**
 *
 * @author Kasper Piskorski
 *
 */
public class GenealogyKB extends TestKB {

    final private static String schemaFile = "genealogy/schema.gql";
    final private static String dataFile = "genealogy/data.gql";
    final private static String rulesFile = "genealogy/rules.gql";

    public static SampleKBContext context() {
        return new GenealogyKB().makeContext();
    }

    public static Consumer<GraknTx> get() {
        return new GenealogyKB().build();
    }

    @Override
    public Consumer<GraknTx> build(){
        return (GraknTx graph) -> {
            SampleKBLoader.loadFromFile(graph, schemaFile);
            SampleKBLoader.loadFromFile(graph, dataFile);
            SampleKBLoader.loadFromFile(graph, rulesFile);
        };
    }
}
