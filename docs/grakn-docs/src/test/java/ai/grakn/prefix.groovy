package ai.grakn

import ai.grakn.concept.*
import ai.grakn.example.*
import ai.grakn.graql.*
import static ai.grakn.graql.Graql.*;
// This is some dumb stuff so IntelliJ doesn't get rid of the imports
//noinspection GroovyConstantIfStatement
if (false) {
    Concept concept = null;
    Var var = var();
    PokemonGraphFactory.loadGraph(null);
}

GraknGraph graknGraph = DocTestUtil.getTestGraph();
QueryBuilder qb = graknGraph.graql();
