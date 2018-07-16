const META_TYPE = "META_TYPE";
const ATTRIBUTE_TYPE = "ATTRIBUTE_TYPE";
const RELATIONSHIP_TYPE = "RELATIONSHIP_TYPE";
const ENTITY_TYPE = "ENTITY_TYPE";
const ENTITY = "ENTITY";
const ATTRIBUTE = "ATTRIBUTE";
const RELATIONSHIP = "RELATIONSHIP";
const ROLE = "ROLE";
const RULE = "RULE";

const SCHEMA_CONCEPTS = new Set([RULE, ROLE, ATTRIBUTE_TYPE, RELATIONSHIP_TYPE, ENTITY_TYPE]);
const TYPES = new Set([ATTRIBUTE_TYPE, RELATIONSHIP_TYPE, ENTITY_TYPE]);
const THINGS = new Set([ATTRIBUTE, RELATIONSHIP, ATTRIBUTE, ENTITY]);

const methods = function () {
  return {
    delete: function () { return this.txService.deleteConcept(this.id); },
    isSchemaConcept: function () { return SCHEMA_CONCEPTS.has(this.baseType); },
    isType: function () { return TYPES.has(this.baseType); },
    isThing: function () { return THINGS.has(this.baseType); },
    isAttributeType: function () { return this.baseType === ATTRIBUTE_TYPE; },
    isEntityType: function () { return this.baseType === ENTITY_TYPE; },
    isRelationshipType: function () { return this.baseType === RELATIONSHIP_TYPE; },
    isRole: function () { return this.baseType === ROLE; },
    isRule: function () { return this.baseType === RULE; },
    isAttribute: function () { return this.baseType === ATTRIBUTE; },
    isEntity: function () { return this.baseType === ENTITY; },
    isRelationship: function () { return this.baseType === RELATIONSHIP; }
  };
};

module.exports = {
  get: function (baseType) { return methods(baseType); },
  META_TYPE,
  ATTRIBUTE,
  ATTRIBUTE_TYPE,
  ROLE,
  RULE,
  RELATIONSHIP,
  RELATIONSHIP_TYPE,
  ENTITY,
  ENTITY_TYPE
};
