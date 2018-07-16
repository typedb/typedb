
const methods = {
  isAbstract: function (bool) {
    if (bool != null) return this.txService.setAbstract(this.id, bool);
    else return this.txService.isAbstract(this.id);
  },
  plays: function (role) { return this.txService.setRolePlayedByType(this.id, role); },
  playing: function () { return this.txService.getRolesPlayedByType(this.id); },
  key: function (attributeType) { return this.txService.setKeyType(this.id, attributeType); },
  has: function (attributeType) { return this.txService.setAttributeType(this.id, attributeType); },
  attributes: function () { return this.txService.getAttributeTypes(this.id); },
  keys: function () { return this.txService.getKeyTypes(this.id); },
  instances: function () { return this.txService.instances(this.id); },
  unplay: function (role) { return this.txService.unsetRolePlayedByType(this.id, role); },
  unhas: function (attributeType) { return this.txService.unsetAttributeType(this.id, attributeType); },
  unkey: function (attributeType) { return this.txService.unsetKeyType(this.id, attributeType); },
};

module.exports = {
  get: function () {
    return methods;
  }
};
