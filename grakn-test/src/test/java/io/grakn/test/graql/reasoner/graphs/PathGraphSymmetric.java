package io.grakn.test.graql.reasoner.graphs;

import io.grakn.GraknGraph;

public class PathGraphSymmetric extends PathGraph{

    public static GraknGraph getGraph(int n, int children) {
        final String gqlFile = "path-test-symmetric.gql";
        getGraph(gqlFile);
        buildExtensionalDB(n, children);
        commit();
        return grakn;
    }
}
