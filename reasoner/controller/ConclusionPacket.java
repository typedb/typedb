/*
 * Copyright (C) 2021 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.reasoner.controller;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;

import java.util.Collection;
import java.util.Objects;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class ConclusionPacket {

    public boolean isMaterialisationBounds() {
        return false;
    }

    public MaterialisationBounds asMaterialisationBounds() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    public boolean isMaterialisationAnswer() {
        return false;
    }

    public MaterialisationAnswer asMaterialisationAnswer() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    public boolean isConditionAnswer() {
        return false;
    }

    public ConditionAnswer asConditionAnswer() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    public boolean isConditionBounds() {
        return false;
    }

    public ConditionBounds asConditionBounds() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    public ConclusionPacket asConclusionPacket() {
        return this;
    }

    public static class MaterialisationBounds extends ConclusionPacket {  // TODO: Don't need this now, use ConceptMap
        private final ConceptMap conceptMap;
        private final Rule.Conclusion conclusion;
        private final Collection<? extends Concept> concepts;

        MaterialisationBounds(ConceptMap conceptMap, Rule.Conclusion conclusion) {
            this.conceptMap = conceptMap;
            this.conclusion = conclusion;
            this.concepts = conceptMap.concepts().values();
        }

        public ConceptMap conceptMap() {
            return conceptMap;
        }

        public Rule.Conclusion conclusion() {
            return conclusion;
        }

        public boolean isMaterialisationBounds() {
            return true;
        }

        @Override
        public MaterialisationBounds asMaterialisationBounds() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MaterialisationBounds that = (MaterialisationBounds) o;
            return conceptMap.equals(that.conceptMap) &&
                    conclusion.equals(that.conclusion) &&
                    concepts.equals(that.concepts);
        }

        @Override
        public int hashCode() {
            return Objects.hash(conceptMap, conclusion, concepts);
        }
    }

    public static class MaterialisationAnswer extends ConclusionPacket {
        private final VarConceptMap concepts;

        MaterialisationAnswer(VarConceptMap concepts) {
            this.concepts = concepts;
        }

        VarConceptMap concepts() {
            return concepts;
        }

        public boolean isMaterialisationAnswer() {
            return true;
        }

        public MaterialisationAnswer asMaterialisationAnswer() {
            return this;
        }
    }

    public static class ConditionAnswer extends ConclusionPacket {

        private final ConceptMap conceptMap;

        public ConditionAnswer(ConceptMap conceptMap) {
            this.conceptMap = conceptMap;
        }

        public ConceptMap conceptMap() {
            return conceptMap;
        }

        public boolean isConditionAnswer() {
            return true;
        }

        public ConditionAnswer asConditionAnswer() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConditionAnswer that = (ConditionAnswer) o;
            return conceptMap.equals(that.conceptMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(conceptMap);
        }
    }

    public static class ConditionBounds extends ConclusionPacket {  // TODO: Don't need this now, use ConceptMap

        private final ConceptMap conceptMap;

        public ConditionBounds(ConceptMap conceptMap) {
            this.conceptMap = conceptMap;
        }

        public ConceptMap conceptMap() {
            return conceptMap;
        }

        public boolean isConditionBounds() {
            return true;
        }

        public ConditionBounds asConditionBounds() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConditionBounds that = (ConditionBounds) o;
            return conceptMap.equals(that.conceptMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(conceptMap);
        }
    }
}
