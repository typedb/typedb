
const methods = {
  create: function (value) { return this.txService.putAttribute(this.id, value); },
  attribute: function (value) { return this.txService.getAttribute(this.id, value); },
  dataType: function () { return this.txService.getDataTypeOfType(this.id); },
  regex: function (regex) {
    if (regex) return this.txService.setRegex(this.id, regex);
    else return this.txService.getRegex(this.id);
  },
};

module.exports = {
  get: function () {
    return methods;
  }
};
