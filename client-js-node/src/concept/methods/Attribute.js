
const methods = {
  dataType: function () { return this.txService.getDataTypeOfAttribute(this.id); },
  getValue: function () { return this.txService.getValue(this.id); },
  ownerInstances: function () { return this.txService.getOwners(this.id); }
};

module.exports = {
  get: function () {
    return methods;
  }
};
