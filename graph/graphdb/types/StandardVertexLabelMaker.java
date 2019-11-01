// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.graphdb.types;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.VertexLabel;
import grakn.core.graph.core.schema.VertexLabelMaker;
import grakn.core.graph.graphdb.internal.JanusGraphSchemaCategory;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.types.system.SystemTypeManager;

import static grakn.core.graph.graphdb.types.TypeDefinitionCategory.PARTITIONED;
import static grakn.core.graph.graphdb.types.TypeDefinitionCategory.STATIC;

public class StandardVertexLabelMaker implements VertexLabelMaker {

    private final StandardJanusGraphTx tx;

    private String name;
    private boolean partitioned;
    private boolean isStatic;

    public StandardVertexLabelMaker(StandardJanusGraphTx tx) {
        this.tx = tx;
    }

    public StandardVertexLabelMaker name(String name) {
        //Verify name
        SystemTypeManager.throwIfSystemName(JanusGraphSchemaCategory.VERTEXLABEL, name);
        this.name = name;
        return this;
    }


    public String getName() {
        return name;
    }

    @Override
    public StandardVertexLabelMaker partition() {
        partitioned = true;
        return this;
    }

    @Override
    public StandardVertexLabelMaker setStatic() {
        isStatic = true;
        return this;
    }

    @Override
    public VertexLabel make() {
        Preconditions.checkArgument(!partitioned || !isStatic, "A vertex label cannot be partitioned and static at the same time");
        TypeDefinitionMap def = new TypeDefinitionMap();
        def.setValue(PARTITIONED, partitioned);
        def.setValue(STATIC, isStatic);

        return (VertexLabelVertex) tx.makeSchemaVertex(JanusGraphSchemaCategory.VERTEXLABEL, name, def);
    }
}
