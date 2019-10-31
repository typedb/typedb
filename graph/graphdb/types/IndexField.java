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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.janusgraph.core.PropertyKey;


public class IndexField {

    private final PropertyKey key;

    IndexField(PropertyKey key) {
        this.key = Preconditions.checkNotNull(key);
    }

    public PropertyKey getFieldKey() {
        return key;
    }

    public static org.janusgraph.graphdb.types.IndexField of(PropertyKey key) {
        return new org.janusgraph.graphdb.types.IndexField(key);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key);
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        else if (oth==null || !getClass().isInstance(oth)) return false;
        org.janusgraph.graphdb.types.IndexField other = (org.janusgraph.graphdb.types.IndexField)oth;
        if (key==null) return key==other.key;
        else return key.equals(other.key);
    }

    @Override
    public String toString() {
        return "["+key.name()+"]";
    }

}
