/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core.implementation;

import io.mindmaps.core.exceptions.ConceptException;
import io.mindmaps.core.model.Type;

public class MockConcept extends ConceptImpl {
    public MockConcept(){
        super(null, null);
    }

    @Override
    public void delete() throws ConceptException {

    }

    @Override
    public String getProperty(DataType.ConceptPropertyUnique key) {
        return null;
    }

    @Override
    public Object getProperty(DataType.ConceptProperty key) {
        return null;
    }

    @Override
    public long getBaseIdentifier() {
        return 0;
    }

    @Override
    public String getBaseType() {
        return null;
    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public String getSubject() {
        return null;
    }

    @Override
    public String getType() {
        return null;
    }

    @Override
    public Object getValue() {
        return null;
    }

    @Override
    public Type type() {
        return null;
    }

    @Override
    public boolean isAlive() {
        return false;
    }
}
