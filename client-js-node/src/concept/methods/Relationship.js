
const methods = {
  rolePlayersMap: async function () {
    return this.txService.rolePlayersMap(this.id);
  },
  rolePlayers: async function (...roles) {
    return this.txService.rolePlayers(this.id, roles);
  },
  addRolePlayer: function (role, thing) { return this.txService.setRolePlayer(this.id, role, thing); },
  removeRolePlayer: function (role, thing) { return this.txService.unsetRolePlayer(this.id, role, thing); }
};

module.exports = {
  get: function () {
    return methods;
  }
};
