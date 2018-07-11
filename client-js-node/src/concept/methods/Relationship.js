
const methods = {
  allRolePlayers: async function () {
    const rolePlayers = await this.txService.getRolePlayers(this.id);
    // Temp map to store String id to Role object
    const tempMap = new Map(rolePlayers.map(entry => [entry.role.id, entry.role]));
    const map = new Map();
    // Create map using string as key and set as value
    rolePlayers.forEach(rp => {
      const key = rp.role.id;
      if (map.has(key)) map.set(key, map.get(key).add(rp.player));
      else map.set(key, new Set([rp.player]));
    })
    const resultMap = new Map();
    // Convert map to use Role object as key
    map.forEach((value, key) => {
      resultMap.set(tempMap.get(key), value);
    });
    return resultMap;
  },
  rolePlayers: async function (...roles) {
    if (roles.length > 0) {
      return this.txService.getRolePlayersByRoles(this.id, roles);
    } else {
      const rolePlayers = await this.txService.getRolePlayers(this.id);
      const playersArray = rolePlayers.map(entry => entry.player);
      //Helper array to remove duplicates
      const idsArray = rolePlayers.map(entry => entry.player.id);
      //Filter out duplicates from playersArray
      return playersArray.filter((value, i) => i === idsArray.indexOf(value.id));
    }
  },
  addRolePlayer: function (role, thing) { return this.txService.setRolePlayer(this.id, role, thing); },
  removeRolePlayer: function (role, thing) { return this.txService.unsetRolePlayer(this.id, role, thing); }
};

module.exports = {
  get: function () {
    return methods;
  }
};
