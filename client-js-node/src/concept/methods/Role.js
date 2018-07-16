
const methods = {
  relationships: function () { return this.txService.getRelationshipTypesThatRelateRole(this.id); },
  players: function () { return this.txService.getTypesThatPlayRole(this.id); },
};

module.exports = {
  get: function () {
    return methods;
  }
};
