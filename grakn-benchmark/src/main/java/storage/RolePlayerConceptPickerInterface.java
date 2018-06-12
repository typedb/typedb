package storage;

import ai.grakn.GraknTx;
import pdf.PDF;

import java.util.stream.Stream;

public interface RolePlayerConceptPickerInterface {
    Stream<String> get(PDF pdf, GraknTx tx);
    void reset();
}
