package strategy;

import ai.grakn.concept.Type;
import pdf.PDF;

public interface TypeStrategyInterface<T extends Type> {
    T getType();

    String getTypeLabel();

    PDF getNumInstancesPDF();
}
