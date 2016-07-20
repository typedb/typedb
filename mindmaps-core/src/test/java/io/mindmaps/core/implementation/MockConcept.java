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
