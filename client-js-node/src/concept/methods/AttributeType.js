
const methods = {
  putAttribute: function (value) { return this.txService.putAttribute(this.id, value); },
  getAttribute: function (value) { return this.txService.getAttribute(this.id, value); },
  getDataType: function () { return this.txService.getDataTypeOfType(this.id); },
  getRegex: function () { return this.txService.getRegex(this.id); },
  setRegex: function (regex) { return this.txService.setRegex(this.id, regex); }
};

module.exports = {
  get: function () {
    return methods;
  }
};
