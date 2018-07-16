
const methods = {
  getWhen: function () { return this.txService.getWhen(this.id); },
  getThen: function () { return this.txService.getThen(this.id); }
};

module.exports = {
  get: function () {
    return methods;
  }
};
