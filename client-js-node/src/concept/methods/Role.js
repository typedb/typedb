
const methods = {
  relationshipTypes: function () { return this.txService.getRelationshipTypesThatRelateRole(this.id); },
  playedByTypes: function () { return this.txService.getTypesThatPlayRole(this.id); },
};

module.exports = {
  get: function () {
    return methods;
  }
};
