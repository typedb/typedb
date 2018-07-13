const methods = {
  isInferred: function () { return this.txService.isInferred(this.id); },
  type: function () { return this.txService.getDirectType(this.id); },
  relationships: function (...roles) {
    return this.txService.getRelationshipsByRoles(this.id, roles);
  },
  plays: function () { return this.txService.getRolesPlayedByThing(this.id); },
  attributes: function (...attributes) {
    return this.txService.getAttributesByTypes(this.id, attributes);
  },
  keys: function (...types) {
    return this.txService.getKeysByTypes(this.id, types);
  },
  // Note: in Java Core API this method is called `attributeRelationship`
  // because the `attribute` method has a behaviour that does not apply to JS.
  // So in here we just have `attribute`.
  attribute: function (attribute) { return this.txService.setAttribute(this.id, attribute); },
  deleteAttribute: function (attribute) { return this.txService.unsetAttribute(this.id, attribute); }
};

module.exports = {
  get: function () {
    return methods;
  }
};
