/*
 * Copyright (C) 2020 Grakn Labs
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
 */

package grakn.core.graql.reasoner.atom.task.validate;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import graql.lang.statement.Variable;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class IsaAtomValidator implements AtomValidator<IsaAtom> {

    private final BasicAtomValidator basicValidator;

    public IsaAtomValidator(){
        this.basicValidator = new BasicAtomValidator();
    }

    @Override
    public void checkValid(IsaAtom atom, ReasoningContext ctx) {
        basicValidator.checkValid(atom, ctx);
    }

    @Override
    public Set<String> validateAsRuleHead(IsaAtom atom, Rule rule, ReasoningContext ctx) {
        /*
        An IsaAtom is ok as the head of a rule when:

        1. The atom is NOT a relation type (must specify roles, making it a RelationAtom
        2. If it is an attribute type, the datatype must match
        3. Types between rule head and body must be of same meta type
        */

        Set<String> errors = new HashSet<>();

        SchemaConcept type = atom.getSchemaConcept();
        if (type.isRelationType()) {
            errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_REWRITING_TYPE_TO_RELATION.getMessage(rule.label()));
        } else if (type.isAttributeType()) {
            AttributeType.DataType<Object> datatypeInHead = type.asAttributeType().dataType();

            // parent query is the combined rule head and body

            boolean datatypeMatchesBody = atom.getParentQuery().getAtoms(Atom.class)
                    .filter(ruleAtom -> ruleAtom instanceof AttributeAtom || ruleAtom instanceof IsaAtom)
                    .map(ruleAtom -> ruleAtom.getSchemaConcept())
                    .filter(Objects::nonNull)
                    .filter(SchemaConcept::isAttributeType)
                    .anyMatch(schemaConcept -> schemaConcept.asAttributeType().dataType().equals(datatypeInHead));

            if (!datatypeMatchesBody) {
                errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_REWRITING_TYPE_DATATYPE_INCOMPATIBLE.getMessage(rule, datatypeInHead));
            }
        } else {
            boolean typeMatchesBody = atom.getParentQuery().getAtoms(IsaAtom.class)
                    .map(isaAtom -> isaAtom.getSchemaConcept())
                    .filter(Objects::nonNull)
                    .anyMatch(schemaConcept -> type.isAttributeType() && schemaConcept.isAttributeType() ||
                                        type.isEntityType() && schemaConcept.isEntityType() ||
                                        type.isAttributeType() && schemaConcept.isAttributeType());

            if (!typeMatchesBody) {
                Variable headVariable = atom.getVarName();
                errors.add(ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_REWRITING_META_TYPE.getMessage(rule, headVariable));
            }
        }

        return null;
    }

    @Override
    public Set<String> validateAsRuleBody(IsaAtom atom, Label ruleLabel, ReasoningContext ctx) {
        return new HashSet<>();
    }
}
