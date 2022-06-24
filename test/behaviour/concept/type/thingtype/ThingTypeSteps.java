/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.test.behaviour.concept.type.thingtype;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.test.behaviour.config.Parameters;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.common.test.Util.assertThrows;
import static com.vaticle.typedb.core.test.behaviour.config.Parameters.RootLabel;
import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.tx;
import static java.util.Objects.isNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Behaviour Steps generic to all ThingTypes
 */
public class ThingTypeSteps {

    public static ThingType get_thing_type(RootLabel rootLabel, String typeLabel) {
        switch (rootLabel) {
            case ENTITY:
                return tx().concepts().getEntityType(typeLabel);
            case ATTRIBUTE:
                return tx().concepts().getAttributeType(typeLabel);
            case RELATION:
                return tx().concepts().getRelationType(typeLabel);
            default:
                throw TypeDBException.of(UNRECOGNISED_VALUE);
        }
    }

    @Then("thing type root get supertypes contain:")
    public void thing_type_root_get_supertypes_contain(List<String> superLabels) {
        Set<String> actuals = tx().concepts().getRootThingType().getSupertypes().map(ThingType::getLabel).map(Label::toString).toSet();
        assertTrue(actuals.containsAll(superLabels));
    }

    @Then("thing type root get supertypes do not contain:")
    public void thing_type_root_get_supertypes_do_not_contain(List<String> superLabels) {
        Set<String> actuals = tx().concepts().getRootThingType().getSupertypes().map(ThingType::getLabel).map(Label::toString).toSet();
        for (String superLabel : superLabels) {
            assertFalse(actuals.contains(superLabel));
        }
    }

    @Then("thing type root get subtypes contain:")
    public void thing_type_root_get_subtypes_contain(List<String> subLabels) {
        Set<String> actuals = tx().concepts().getRootThingType().getSubtypes().map(ThingType::getLabel).map(Label::toString).toSet();
        assertTrue(actuals.containsAll(subLabels));
    }

    @Then("thing type root get subtypes do not contain:")
    public void thing_type_root_get_subtypes_do_not_contain(List<String> subLabels) {
        Set<String> actuals = tx().concepts().getRootThingType().getSubtypes().map(ThingType::getLabel).map(Label::toString).toSet();
        for (String subLabel : subLabels) {
            assertFalse(actuals.contains(subLabel));
        }
    }

    @When("put {root_label} type: {type_label}")
    public void put_thing_type(RootLabel rootLabel, String typeLabel) {
        switch (rootLabel) {
            case ENTITY:
                tx().concepts().putEntityType(typeLabel);
                break;
            case RELATION:
                tx().concepts().putRelationType(typeLabel);
                break;
            default:
                throw TypeDBException.of(UNRECOGNISED_VALUE);
        }
    }

    @When("delete {root_label} type: {type_label}")
    public void delete_thing_type(RootLabel rootLabel, String typeLabel) {
        get_thing_type(rootLabel, typeLabel).delete();
    }

    @Then("delete {root_label} type: {type_label}; throws exception")
    public void delete_thing_type_throws_exception(RootLabel rootLabel, String typeLabel) {
        assertThrows(() -> get_thing_type(rootLabel, typeLabel).delete());
    }

    @Then("{root_label}\\( ?{type_label} ?) is null: {bool}")
    public void thing_type_is_null(RootLabel rootLabel, String typeLabel, boolean isNull) {
        assertEquals(isNull, isNull(get_thing_type(rootLabel, typeLabel)));
    }

    @When("{root_label}\\( ?{type_label} ?) set label: {type_label}")
    public void thing_type_set_label(RootLabel rootLabel, String typeLabel, String newLabel) {
        get_thing_type(rootLabel, typeLabel).setLabel(newLabel);
    }

    @Then("{root_label}\\( ?{type_label} ?) get label: {type_label}")
    public void thing_type_get_label(RootLabel rootLabel, String typeLabel, String getLabel) {
        assertEquals(getLabel, get_thing_type(rootLabel, typeLabel).getLabel().toString());
    }

    @When("{root_label}\\( ?{type_label} ?) set abstract: {bool}")
    public void thing_type_set_abstract(RootLabel rootLabel, String typeLabel, boolean isAbstract) {
        if (isAbstract) get_thing_type(rootLabel, typeLabel).setAbstract();
        else get_thing_type(rootLabel, typeLabel).unsetAbstract();
    }

    @When("{root_label}\\( ?{type_label} ?) set abstract: {bool}; throws exception")
    public void thing_type_set_abstract_throws(RootLabel rootLabel, String typeLabel, boolean isAbstract) {
        if (isAbstract) assertThrows(() -> get_thing_type(rootLabel, typeLabel).setAbstract());
        else assertThrows(() -> get_thing_type(rootLabel, typeLabel).unsetAbstract());
    }

    @Then("{root_label}\\( ?{type_label} ?) is abstract: {bool}")
    public void thing_type_is_abstract(RootLabel rootLabel, String typeLabel, boolean isAbstract) {
        assertEquals(isAbstract, get_thing_type(rootLabel, typeLabel).isAbstract());
    }

    @When("{root_label}\\( ?{type_label} ?) set supertype: {type_label}")
    public void thing_type_set_supertype(RootLabel rootLabel, String typeLabel, String superLabel) {
        switch (rootLabel) {
            case ENTITY:
                EntityType entitySuperType = tx().concepts().getEntityType(superLabel);
                tx().concepts().getEntityType(typeLabel).setSupertype(entitySuperType);
                break;
            case ATTRIBUTE:
                AttributeType attributeSuperType = tx().concepts().getAttributeType(superLabel);
                tx().concepts().getAttributeType(typeLabel).setSupertype(attributeSuperType);
                break;
            case RELATION:
                RelationType relationSuperType = tx().concepts().getRelationType(superLabel);
                tx().concepts().getRelationType(typeLabel).setSupertype(relationSuperType);
                break;
        }
    }

    @Then("{root_label}\\( ?{type_label} ?) set supertype: {type_label}; throws exception")
    public void thing_type_set_supertype_throws_exception(RootLabel rootLabel, String typeLabel, String superLabel) {
        switch (rootLabel) {
            case ENTITY:
                EntityType entitySuperType = tx().concepts().getEntityType(superLabel);
                assertThrows(() -> tx().concepts().getEntityType(typeLabel).setSupertype(entitySuperType));
                break;
            case ATTRIBUTE:
                AttributeType attributeSuperType = tx().concepts().getAttributeType(superLabel);
                assertThrows(() -> tx().concepts().getAttributeType(typeLabel).setSupertype(attributeSuperType));
                break;
            case RELATION:
                RelationType relationSuperType = tx().concepts().getRelationType(superLabel);
                assertThrows(() -> tx().concepts().getRelationType(typeLabel).setSupertype(relationSuperType));
                break;
        }
    }

    @Then("{root_label}\\( ?{type_label} ?) get supertype: {type_label}")
    public void thing_type_get_supertype(RootLabel rootLabel, String typeLabel, String superLabel) {
        ThingType supertype = get_thing_type(rootLabel, superLabel);
        assertEquals(supertype, get_thing_type(rootLabel, typeLabel).getSupertype());
    }

    @Then("{root_label}\\( ?{type_label} ?) get supertypes contain:")
    public void thing_type_get_supertypes_contain(RootLabel rootLabel, String typeLabel, List<String> superLabels) {
        Set<String> actuals = get_thing_type(rootLabel, typeLabel).getSupertypes().map(ThingType::getLabel).map(Label::toString).toSet();
        assertTrue(actuals.containsAll(superLabels));
    }

    @Then("{root_label}\\( ?{type_label} ?) get supertypes do not contain:")
    public void thing_type_get_supertypes_do_not_contain(RootLabel rootLabel, String typeLabel, List<String> superLabels) {
        Set<String> actuals = get_thing_type(rootLabel, typeLabel).getSupertypes().map(ThingType::getLabel).map(Label::toString).toSet();
        for (String superLabel : superLabels) {
            assertFalse(actuals.contains(superLabel));
        }
    }

    @Then("{root_label}\\( ?{type_label} ?) get subtypes contain:")
    public void thing_type_get_subtypes_contain(RootLabel rootLabel, String typeLabel, List<String> subLabels) {
        Set<String> actuals = get_thing_type(rootLabel, typeLabel).getSubtypes().map(ThingType::getLabel).map(Label::toString).toSet();
        assertTrue(actuals.containsAll(subLabels));
    }

    @Then("{root_label}\\( ?{type_label} ?) get subtypes do not contain:")
    public void thing_type_get_subtypes_do_not_contain(RootLabel rootLabel, String typeLabel, List<String> subLabels) {
        Set<String> actuals = get_thing_type(rootLabel, typeLabel).getSubtypes().map(ThingType::getLabel).map(Label::toString).toSet();
        for (String subLabel : subLabels) {
            assertFalse(actuals.contains(subLabel));
        }
    }

    @When("{root_label}\\( ?{type_label} ?) set owns key type: {type_label}")
    public void thing_type_set_owns_key_type(RootLabel rootLabel, String typeLabel, String attTypeLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attTypeLabel);
        get_thing_type(rootLabel, typeLabel).setOwns(attributeType, true);
    }

    @When("{root_label}\\( ?{type_label} ?) set owns key type: {type_label} as {type_label}")
    public void thing_type_set_owns_key_type_as(RootLabel rootLabel, String typeLabel, String attTypeLabel, String overriddenLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attTypeLabel);
        AttributeType overriddenType = tx().concepts().getAttributeType(overriddenLabel);
        get_thing_type(rootLabel, typeLabel).setOwns(attributeType, overriddenType, true);
    }

    @Then("{root_label}\\( ?{type_label} ?) set owns key type: {type_label}; throws exception")
    public void thing_type_set_owns_key_type_throws_exception(RootLabel rootLabel, String typeLabel, String attributeLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        assertThrows(() -> get_thing_type(rootLabel, typeLabel).setOwns(attributeType, true));
    }

    @Then("{root_label}\\( ?{type_label} ?) set owns key type: {type_label} as {type_label}; throws exception")
    public void thing_type_set_owns_key_type_as_throws_exception(RootLabel rootLabel, String typeLabel, String attributeLabel, String overriddenLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        AttributeType overriddenType = tx().concepts().getAttributeType(overriddenLabel);
        assertThrows(() -> get_thing_type(rootLabel, typeLabel).setOwns(attributeType, overriddenType, true));
    }

    @When("{root_label}\\( ?{type_label} ?) unset owns key type: {type_label}")
    public void thing_type_unset_owns_key_type(RootLabel rootLabel, String typeLabel, String attributeLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        get_thing_type(rootLabel, typeLabel).unsetOwns(attributeType);
    }

    @Then("{root_label}\\( ?{type_label} ?) get owns key types contain:")
    public void thing_type_get_owns_key_types_contain(RootLabel rootLabel, String typeLabel, List<String> attributeLabels) {
        Set<String> actuals = get_thing_type(rootLabel, typeLabel).getOwns(true).map(Type::getLabel).map(Label::toString).toSet();
        assertTrue(actuals.containsAll(attributeLabels));
    }

    @Then("{root_label}\\( ?{type_label} ?) get owns key types do not contain:")
    public void thing_type_get_owns_key_types_do_not_contain(RootLabel rootLabel, String typeLabel, List<String> attributeLabels) {
        Set<String> actuals = get_thing_type(rootLabel, typeLabel).getOwns(true).map(Type::getLabel).map(Label::toString).toSet();
        for (String attributeLabel : attributeLabels) {
            assertFalse(actuals.contains(attributeLabel));
        }
    }

    @Then("{root_label}\\( ?{type_label} ?) get owns explicit key types contain:")
    public void thing_type_get_owns_explicit_key_types_contain(RootLabel rootLabel, String typeLabel, List<String> attributeLabels) {
        Set<String> actuals = get_thing_type(rootLabel, typeLabel).getOwnsExplicit(true).map(Type::getLabel).map(Label::toString).toSet();
        assertTrue(actuals.containsAll(attributeLabels));
    }

    @Then("{root_label}\\( ?{type_label} ?) get owns explicit key types do not contain:")
    public void thing_type_get_owns_explicit_key_types_do_not_contain(RootLabel rootLabel, String typeLabel, List<String> attributeLabels) {
        Set<String> actuals = get_thing_type(rootLabel, typeLabel).getOwnsExplicit(true).map(Type::getLabel).map(Label::toString).toSet();
        for (String attributeLabel : attributeLabels) {
            assertFalse(actuals.contains(attributeLabel));
        }
    }

    @When("{root_label}\\( ?{type_label} ?) set owns attribute type: {type_label}")
    public void thing_type_set_owns_attribute_type(RootLabel rootLabel, String typeLabel, String attributeLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        get_thing_type(rootLabel, typeLabel).setOwns(attributeType);
    }

    @Then("{root_label}\\( ?{type_label} ?) set owns attribute type: {type_label}; throws exception")
    public void thing_type_set_owns_attribute_throws_exception(RootLabel rootLabel, String typeLabel, String attributeLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        assertThrows(() -> get_thing_type(rootLabel, typeLabel).setOwns(attributeType));
    }

    @When("{root_label}\\( ?{type_label} ?) set owns attribute type: {type_label} as {type_label}")
    public void thing_type_set_owns_attribute_type_as(RootLabel rootLabel, String typeLabel, String attributeLabel, String overriddenLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        AttributeType overriddenType = tx().concepts().getAttributeType(overriddenLabel);
        get_thing_type(rootLabel, typeLabel).setOwns(attributeType, overriddenType);
    }

    @Then("{root_label}\\( ?{type_label} ?) set owns attribute type: {type_label} as {type_label}; throws exception")
    public void thing_type_set_owns_attribute_as_throws_exception(RootLabel rootLabel, String typeLabel, String attributeLabel, String overriddenLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        AttributeType overriddenType = tx().concepts().getAttributeType(overriddenLabel);
        assertThrows(() -> get_thing_type(rootLabel, typeLabel).setOwns(attributeType, overriddenType));
    }

    @When("{root_label}\\( ?{type_label} ?) unset owns attribute type: {type_label}")
    public void thing_type_unset_owns_attribute_type(RootLabel rootLabel, String typeLabel, String attributeLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        get_thing_type(rootLabel, typeLabel).unsetOwns(attributeType);
    }

    @When("{root_label}\\( ?{type_label} ?) unset owns attribute type: {type_label}; throws exception")
    public void thing_type_unset_owns_attribute_type_throws_exception(RootLabel rootLabel, String typeLabel, String attributeLabel) {
        assertThrows(() -> thing_type_unset_owns_attribute_type(rootLabel, typeLabel, attributeLabel));
    }

    @Then("{root_label}\\( ?{type_label} ?) get owns overridden attribute\\( ?{type_label} ?) is null: {bool}")
    public void thing_type_get_owns_overridden_attribute_is_null(RootLabel rootLabel, String typeLabel, String attributeLabel, boolean isNull) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        assertEquals(isNull, isNull(get_thing_type(rootLabel, typeLabel).getOwnsOverridden(attributeType)));
    }

    @Then("{root_label}\\( ?{type_label} ?) get owns overridden attribute\\( ?{type_label} ?) get label: {type_label}")
    public void thing_type_get_owns_overridden_attribute_get_label(RootLabel rootLabel, String typeLabel, String attributeLabel, String getLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        assertEquals(getLabel, get_thing_type(rootLabel, typeLabel).getOwnsOverridden(attributeType).getLabel().name());
    }

    @Then("{root_label}\\( ?{type_label} ?) get owns attribute types contain:")
    public void thing_type_get_owns_attribute_types_contain(RootLabel rootLabel, String typeLabel, List<String> attributeLabels) {
        Set<String> actuals = get_thing_type(rootLabel, typeLabel).getOwns().map(Type::getLabel).map(Label::toString).toSet();
        assertTrue(actuals.containsAll(attributeLabels));
    }

    @Then("{root_label}\\( ?{type_label} ?) get owns attribute types do not contain:")
    public void thing_type_get_owns_attribute_types_do_not_contain(RootLabel rootLabel, String typeLabel, List<String> attributeLabels) {
        Set<String> actuals = get_thing_type(rootLabel, typeLabel).getOwns().map(Type::getLabel).map(Label::toString).toSet();
        for (String attributeLabel : attributeLabels) {
            assertFalse(actuals.contains(attributeLabel));
        }
    }

    @Then("{root_label}\\( ?{type_label} ?) get owns explicit attribute types contain:")
    public void thing_type_get_owns_explicit_attribute_types_contain(RootLabel rootLabel, String typeLabel, List<String> attributeLabels) {
        Set<String> actuals = get_thing_type(rootLabel, typeLabel).getOwnsExplicit().map(Type::getLabel).map(Label::toString).toSet();
        assertTrue(actuals.containsAll(attributeLabels));
    }

    @Then("{root_label}\\( ?{type_label} ?) get owns explicit attribute types do not contain:")
    public void thing_type_get_owns_explicit_attribute_types_do_not_contain(RootLabel rootLabel, String typeLabel, List<String> attributeLabels) {
        Set<String> actuals = get_thing_type(rootLabel, typeLabel).getOwnsExplicit().map(Type::getLabel).map(Label::toString).toSet();
        for (String attributeLabel : attributeLabels) {
            assertFalse(actuals.contains(attributeLabel));
        }
    }

    @When("{root_label}\\( ?{type_label} ?) set plays role: {scoped_label}")
    public void thing_type_set_plays_role(RootLabel rootLabel, String typeLabel, Parameters.ScopedLabel roleLabel) {
        RoleType roleType = tx().concepts().getRelationType(roleLabel.scope()).getRelates(roleLabel.label());
        get_thing_type(rootLabel, typeLabel).setPlays(roleType);
    }

    @When("{root_label}\\( ?{type_label} ?) set plays role: {scoped_label}; throws exception")
    public void thing_type_set_plays_role_throws_exception(RootLabel rootLabel, String typeLabel, Parameters.ScopedLabel roleLabel) {
        RoleType roleType = tx().concepts().getRelationType(roleLabel.scope()).getRelates(roleLabel.label());
        assertThrows(() -> get_thing_type(rootLabel, typeLabel).setPlays(roleType));
    }

    @When("{root_label}\\( ?{type_label} ?) set plays role: {scoped_label} as {scoped_label}")
    public void thing_type_set_plays_role_as(RootLabel rootLabel, String typeLabel, Parameters.ScopedLabel roleLabel, Parameters.ScopedLabel overriddenLabel) {
        RoleType roleType = tx().concepts().getRelationType(roleLabel.scope()).getRelates(roleLabel.label());
        RoleType overriddenType = tx().concepts().getRelationType(overriddenLabel.scope()).getRelates(overriddenLabel.label());
        get_thing_type(rootLabel, typeLabel).setPlays(roleType, overriddenType);
    }

    @When("{root_label}\\( ?{type_label} ?) set plays role: {scoped_label} as {scoped_label}; throws exception")
    public void thing_type_set_plays_role_as_throws_exception(RootLabel rootLabel, String typeLabel, Parameters.ScopedLabel roleLabel, Parameters.ScopedLabel overriddenLabel) {
        RoleType roleType = tx().concepts().getRelationType(roleLabel.scope()).getRelates(roleLabel.label());
        RoleType overriddenType = tx().concepts().getRelationType(overriddenLabel.scope()).getRelates(overriddenLabel.label());
        assertThrows(() -> get_thing_type(rootLabel, typeLabel).setPlays(roleType, overriddenType));
    }

    @When("{root_label}\\( ?{type_label} ?) unset plays role: {scoped_label}")
    public void thing_type_unset_plays_role(RootLabel rootLabel, String typeLabel, Parameters.ScopedLabel roleLabel) {
        RoleType roleType = tx().concepts().getRelationType(roleLabel.scope()).getRelates(roleLabel.label());
        get_thing_type(rootLabel, typeLabel).unsetPlays(roleType);
    }

    @When("{root_label}\\( ?{type_label} ?) unset plays role: {scoped_label}; throws exception")
    public void thing_type_unset_plays_role_throws_exception(RootLabel rootLabel, String typeLabel, Parameters.ScopedLabel roleLabel) {
        assertThrows(() -> thing_type_unset_plays_role(rootLabel, typeLabel, roleLabel));
    }

    @Then("{root_label}\\( ?{type_label} ?) get playing roles contain:")
    public void thing_type_get_playing_roles_contain(RootLabel rootLabel, String typeLabel, List<Parameters.ScopedLabel> roleLabels) {
        Set<Parameters.ScopedLabel> actuals = get_thing_type(rootLabel, typeLabel).getPlays().map(
                r -> new Parameters.ScopedLabel(r.getLabel())
        ).toSet();
        assertTrue(actuals.containsAll(roleLabels));
    }

    @Then("{root_label}\\( ?{type_label} ?) get playing roles do not contain:")
    public void thing_type_get_playing_roles_do_not_contain(RootLabel rootLabel, String typeLabel, List<Parameters.ScopedLabel> roleLabels) {
        Set<Parameters.ScopedLabel> actuals = get_thing_type(rootLabel, typeLabel).getPlays().map(
                r -> new Parameters.ScopedLabel(r.getLabel())
        ).toSet();
        for (Parameters.ScopedLabel roleLabel : roleLabels) {
            assertFalse(actuals.contains(roleLabel));
        }
    }

    @Then("{root_label}\\( ?{type_label} ?) get playing roles explicit contain:")
    public void thing_type_get_playing_roles_explicit_contain(RootLabel rootLabel, String typeLabel, List<Parameters.ScopedLabel> roleLabels) {
        Set<Parameters.ScopedLabel> actuals = get_thing_type(rootLabel, typeLabel).getPlaysExplicit().map(
                r -> new Parameters.ScopedLabel(r.getLabel())
        ).toSet();
        assertTrue(actuals.containsAll(roleLabels));
    }

    @Then("{root_label}\\( ?{type_label} ?) get playing roles explicit do not contain:")
    public void thing_type_get_playing_roles_explicit_do_not_contain(RootLabel rootLabel, String typeLabel, List<Parameters.ScopedLabel> roleLabels) {
        Set<Parameters.ScopedLabel> actuals = get_thing_type(rootLabel, typeLabel).getPlaysExplicit().map(
                r -> new Parameters.ScopedLabel(r.getLabel())
        ).toSet();
        for (Parameters.ScopedLabel roleLabel : roleLabels) {
            assertFalse(actuals.contains(roleLabel));
        }
    }
}
