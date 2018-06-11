package storage;

import java.util.stream.Stream;

public interface ConceptPicker {
    Stream<String> get(String typeLabel, ConceptStore conceptStore, int next);
}
