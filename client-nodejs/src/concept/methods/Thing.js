const methods = {
  isInferred: function () { return this.txService.isInferred(this.id); },
  type: function () { return this.txService.getDirectType(this.id); },
  relationships: function (...roles) {
    return this.txService.getRelationshipsByRoles(this.id, roles);
  },
  roles: function () { return this.txService.getRolesPlayedByThing(this.id); },
  attributes: function (...attributes) {
    return this.txService.getAttributesByTypes(this.id, attributes);
  },
  keys: function (...types) {
    return this.txService.getKeysByTypes(this.id, types);
  },
  has: function (attribute) { return this.txService.setAttribute(this.id, attribute); },
  unhas: function (attribute) { return this.txService.unsetAttribute(this.id, attribute); }
};

module.exports = {
  get: function () {
    return methods;
  }
};
