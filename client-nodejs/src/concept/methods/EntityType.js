
const methods = {
  create: function () { return this.txService.addEntity(this.id); },
};

module.exports = {
  get: function () {
    return methods;
  }
};
