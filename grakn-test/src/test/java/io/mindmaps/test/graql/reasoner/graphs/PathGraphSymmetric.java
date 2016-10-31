package io.mindmaps.test.graql.reasoner.graphs;

import io.mindmaps.MindmapsGraph;

public class PathGraphSymmetric extends PathGraph{

    public static MindmapsGraph getGraph(int n, int children) {
        final String gqlFile = "path-test-symmetric.gql";
        getGraph(gqlFile);
        buildExtensionalDB(n, children);
        commit();
        return mindmaps;
    }
}
