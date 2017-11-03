/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package ai.grakn.generator;

import ai.grakn.concept.Label;
import com.pholser.junit.quickcheck.generator.GeneratorConfiguration;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Generator that generates totally random type names
 *
 * @author Felix Chapman
 */
public class Labels extends FromTxGenerator<Label> {

    private boolean mustBeUnused = false;

    public Labels() {
        super(Label.class);
        this.fromLastGeneratedTx();
    }

    @Override
    public Label generateFromTx() {
        if (mustBeUnused) {
            Label label;

            int attempts = 0;
            do {
                // After a certain number of attempts, generate truly random strings instead
                if (attempts < 100) {
                    label = metaSyntacticLabel();
                } else {
                    label = trueRandomLabel();
                }
                attempts += 1;
            } while (tx().getSchemaConcept(label) != null);

            return label;
        } else {
            return metaSyntacticLabel();
        }
    }

    public void configure(Unused unused) {
        mustBeUnused();
    }

    public Labels mustBeUnused() {
        mustBeUnused = true;
        return this;
    }

    private Label metaSyntacticLabel() {
        return Label.of(gen().make(MetasyntacticStrings.class).generate(random, status));
    }

    private Label trueRandomLabel() {
        return Label.of(gen(String.class));
    }

    /**
     * Specify that the label should be unused in the graph
     */
    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface Unused {
    }
}
