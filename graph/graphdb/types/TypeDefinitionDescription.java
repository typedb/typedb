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
import org.janusgraph.graphdb.types.TypeDefinitionCategory;


public class TypeDefinitionDescription {

    private final TypeDefinitionCategory category;
    private final Object modifier;

    public TypeDefinitionDescription(TypeDefinitionCategory category, Object modifier) {
        Preconditions.checkNotNull(category);
        if (category.isProperty()) Preconditions.checkArgument(modifier==null);
        else {
            Preconditions.checkArgument(category.isEdge());
            if (category.hasDataType()) Preconditions.checkArgument(modifier==null || modifier.getClass().equals(category.getDataType()));
            else Preconditions.checkArgument(modifier==null);
        }
        this.category = category;
        this.modifier = modifier;
    }

    public static org.janusgraph.graphdb.types.TypeDefinitionDescription of(TypeDefinitionCategory category) {
        return new org.janusgraph.graphdb.types.TypeDefinitionDescription(category,null);
    }

    public TypeDefinitionCategory getCategory() {
        return category;
    }

    public Object getModifier() {
        return modifier;
    }
}
