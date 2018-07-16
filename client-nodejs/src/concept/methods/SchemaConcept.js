
const methods = {
  label: function (label) {
    if (label) return this.txService.setLabel(this.id, label);
    else return this.txService.getLabel(this.id);
  },
  isImplicit: function () { return this.txService.isImplicit(this.id); },
  subs: function () { return this.txService.subs(this.id); },
  sups: function () { return this.txService.sups(this.id); },
  sup: function (type) {
    if (type) {
      return this.txService.setSup(this.id, type);
    } else {
      return this.txService.getSup(this.id);
    }
  }
};

module.exports = {
  get: function () {
    return methods;
  }
};
