
const methods = {
  getLabel: function () { return this.txService.getLabel(this.id); },
  setLabel: function (label) { return this.txService.setLabel(this.id, label); },
  isImplicit: function () { return this.txService.isImplicit(this.id); },
  subs: function () { return this.txService.getSubConcepts(this.id); },
  sups: function () { return this.txService.getSuperConcepts(this.id); },
  sup: function (type) {
    if (type) {
      return this.txService.setDirectSuperConcept(this.id, type);
    } else {
      return this.txService.getDirectSuperConcept(this.id);
    }
  }
};

module.exports = {
  get: function () {
    return methods;
  }
};
