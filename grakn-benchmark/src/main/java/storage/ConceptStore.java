package storage;

import ai.grakn.concept.Concept;

import java.util.ArrayList;

public interface ConceptStore {

    void add(Concept concept);

    ArrayList<String> get(String type);

}
