/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package ai.grakn.test.docs;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknSystemProperty;
import ai.grakn.GraknTxType;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.test.graphs.GenealogyGraph;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;

import java.io.File;
import java.util.Collection;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.fail;

public class DocTestUtil {

    public static final File PAGES = new File(GraknSystemProperty.PROJECT_RELATIVE_DIR.value()+"/docs/pages/");

    public static GraknSession getTestGraph(String uri) {
        String keyspace = UUID.randomUUID().toString().replaceAll("-", "");
        GraknSession session = Grakn.session(uri, keyspace);

        try (GraknGraph graph = session.open(GraknTxType.WRITE)) {
            GenealogyGraph.get().accept(graph);

            // TODO: Remove custom genealogy ontology when not used
            AttributeType<Long> age = graph.putResourceType("age", AttributeType.DataType.LONG);
            graph.getEntityType("person").resource(age);
            graph.putResourceType("nickname", AttributeType.DataType.STRING);

            // TODO: Remove plant ontology when not used
            EntityType plant = graph.putEntityType("plant");
            AttributeType<String> common = graph.putResourceType("common", AttributeType.DataType.STRING);
            AttributeType<String> botanical = graph.putResourceType("botanical", AttributeType.DataType.STRING);
            AttributeType<String> zone = graph.putResourceType("zone", AttributeType.DataType.STRING);
            AttributeType<String> light = graph.putResourceType("light", AttributeType.DataType.STRING);
            AttributeType<Long> availability = graph.putResourceType("availability", AttributeType.DataType.LONG);
            plant.resource(common).resource(botanical).resource(zone).resource(light).resource(availability);

            // TODO: Remove pokemon ontology when not used
            EntityType pokemon = graph.putEntityType("pokemon");
            EntityType pokemonType = graph.putEntityType("pokemon-type");

            AttributeType<String> typeId = graph.putResourceType("type-id", AttributeType.DataType.STRING);
            AttributeType<String> description = graph.putResourceType("description", AttributeType.DataType.STRING);
            AttributeType<Long> pokedexNo = graph.putResourceType("pokedex-no", AttributeType.DataType.LONG);
            AttributeType<Double> weight = graph.putResourceType("weight", AttributeType.DataType.DOUBLE);
            AttributeType<Double> height = graph.putResourceType("height", AttributeType.DataType.DOUBLE);

            graph.putRelationType("has-type")
                    .relates(graph.putRole("type-of-pokemon")).relates(graph.putRole("pokemon-with-type"));

            pokemonType.resource(typeId).resource(description);
            pokemon.resource(weight).resource(height).resource(pokedexNo).resource(description);

            // TODO: Remove these random types when not used
            graph.putEntityType("cluster");

            graph.commit();
        }

        return session;
    }

    public static Collection<File> allMarkdownFiles() {
        return FileUtils.listFiles(PAGES, new RegexFileFilter(".*\\.md"), DirectoryFileFilter.DIRECTORY);
    }

    static void codeBlockFail(String fileAndLine, String codeBlock, String error) {
        fail("Failure in .(" + fileAndLine + ")\n\n" + indent(error) + "\n\nin:\n\n" + indent(codeBlock));
    }

    static String indent(String toIndent) {
        return toIndent.replaceAll("(?m)^", "    ");
    }

    static int getLineNumber(String data, int start) {
        int line = 1;
        Pattern pattern = Pattern.compile("\n");
        Matcher matcher = pattern.matcher(data);
        matcher.region(0, start);
        while(matcher.find()) {
            line++;
        }
        return line;
    }
}
