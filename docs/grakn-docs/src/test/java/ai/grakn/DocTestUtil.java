package ai.grakn;

import ai.grakn.concept.ResourceType;
import ai.grakn.example.MovieGraphFactory;
import ai.grakn.example.PokemonGraphFactory;
import ai.grakn.graql.QueryBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;

import java.io.File;
import java.util.Collection;
import java.util.UUID;

import static ai.grakn.graql.Graql.name;
import static ai.grakn.graql.Graql.var;

public class DocTestUtil {

    public static final String PAGES = "../pages/";

    public static GraknGraph getTestGraph() {
        String keyspace = UUID.randomUUID().toString().replaceAll("-", "");
        GraknGraph graknGraph = Grakn.factory(Grakn.IN_MEMORY, keyspace).getGraph();
        graknGraph.showImplicitConcepts(true);
        PokemonGraphFactory.loadGraph(graknGraph);
        MovieGraphFactory.loadGraph(graknGraph);
        QueryBuilder qb = graknGraph.graql();
        qb.insert(
                var().isa("pokemon").has("name", "Pikachu"),
                var().isa("pokemon-type").has("name", "dragon"),
                name("marriage").sub("relation"),
                name("trainer").sub("role"),
                name("pokemon-trained").sub("role"),
                name("type-id").sub("resource").datatype(ResourceType.DataType.STRING),
                name("pokemon-type").hasResource("type-id")
        ).execute();

        return graknGraph;
    }

    public static Collection<File> allMarkdownFiles() {
        File dir = new File(PAGES);
        return FileUtils.listFiles(dir, new RegexFileFilter(".*\\.md"), DirectoryFileFilter.DIRECTORY);
    }
}
