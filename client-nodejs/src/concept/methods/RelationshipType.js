
const methods = {
  create: function () { return this.txService.addRelationship(this.id); },
  relates: function (role) { return this.txService.setRelatedRole(this.id, role); },
  roles: function () { return this.txService.getRelatedRoles(this.id); },
  unrelate: function (role) { return this.txService.unsetRelatedRole(this.id, role); }
};

module.exports = {
  get: function () {
    return methods;
  }
};
