const { spawnSync } = require('child_process');


const scriptPath = './test/bash/init-grakn.sh';


function startGrakn(){
    const process = spawnSync(scriptPath, ['start'], { stdio: [0, 1, 2] });
    if (process.status != 0) {
      console.log('Failed to start test environment.');
    }
}

function stopGrakn() {
  const process = spawnSync(scriptPath, ['stop'], { stdio: [0, 1, 2] });
  if (process.status != 0) {
    console.log('Failed to stop test environment.');
  }
}

module.exports = { // adapted from: https://git.io/vodU0
  'Dashboard Graph page': function (browser) {
    startGrakn();
    browser
      .url('http://localhost:4567/#/graph')
      .waitForElementVisible('body')
      .assert.title('GRAKN.AI')
      .assert.visible('#grakn-app > .wrapper', 'Check if app has rendered with VueJs')
      .end(stopGrakn);
  },
};
