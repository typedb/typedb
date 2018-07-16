
const methods = {
  dataType: function () { return this.txService.getDataTypeOfAttribute(this.id); },
  value: function () { return this.txService.getValue(this.id); },
  owners: function () { return this.txService.getOwners(this.id); }
};

module.exports = {
  get: function () {
    return methods;
  }
};
