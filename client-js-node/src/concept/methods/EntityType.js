
const methods = {
  addEntity: function () { return this.txService.addEntity(this.id); },
};

module.exports = {
  get: function () {
    return methods;
  }
};
