
const methods = {
  setAbstract: function (bool) { return this.txService.setAbstract(this.id, bool); },
  plays: function (role) {
    if (role) {
      return this.txService.setRolePlayedByType(this.id, role);
    } else {
      return this.txService.getRolesPlayedByType(this.id);
    }
  },
  key: function (attributeType) { return this.txService.setKeyType(this.id, attributeType); },
  attribute: function (attributeType) { return this.txService.setAttributeType(this.id, attributeType); },
  attributes: function () { return this.txService.getAttributeTypes(this.id); },
  keys: function () { return this.txService.getKeyTypes(this.id); },
  instances: function () { return this.txService.instances(this.id); },
  isAbstract: function () { return this.txService.isAbstract(this.id); },
  deletePlays: function (role) { return this.txService.unsetRolePlayedByType(this.id, role); },
  deleteAttribute: function (attributeType) { return this.txService.unsetAttributeType(this.id, attributeType); },
  deleteKey: function (attributeType) { return this.txService.unsetKeyType(this.id, attributeType); },
};

module.exports = {
  get: function () {
    return methods;
  }
};
