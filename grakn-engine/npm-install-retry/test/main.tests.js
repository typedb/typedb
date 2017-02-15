var npm_install_retry = require('../');
var expect            = require('chai').expect;

function Fail3TimesCommand(command) {
  this.times = 0;
  this.command = command;
}
var isWin = /^win/.test(process.platform);
var cbErrorCommand = isWin ? 'echo npm ERR! cb() never called! 1>&2' : 'echo npm ERR\\! cb\\(\\) never called\\! 1>&2';

Fail3TimesCommand.prototype.toString = function() {
  if(this.times < 3) {
    this.times++;
    return this.command;
  }
  return 'echo peace and love';
};

describe('npm-install-retry', function () {

  function testRetries(command) {
    it('should retry after "' + command + '" failed', function (done) {
      npm_install_retry(new Fail3TimesCommand(command), '', { wait: 0, attempts: 10 }, function (err, result) {
        if (err) return done(err);
        expect(result.times).to.eql(4);
        done();
      });
    });
  }

  testRetries(cbErrorCommand);
  testRetries('echo npm ERR! errno ECONNRESET');

  it('should fail if it fail all attempts', function (done) {
    npm_install_retry(cbErrorCommand, '', { wait: 0, attempts: 10 }, function (err, result) {
      expect(err.message).to.eql('too many attempts');
      done();
    });
  });

  it('should have npm_config_color false', function (done) {
    var endOfLine = require('os').EOL;
    var command = isWin ? 'echo %npm_config_color%' : 'echo $npm_config_color';
    npm_install_retry(command, '', { wait: 0, attempts: 10 }, function (err, result) {
      if (err) return done(err);
      expect(result.stdout.split(endOfLine)[0].trim()).to.eql('0');
      done();
    });
  });

});