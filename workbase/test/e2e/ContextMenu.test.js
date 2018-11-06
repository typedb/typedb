const Application = require('spectron').Application;
const assert = require('assert');
const electronPath = require('electron'); // Require Electron from the binaries included in node_modules.
const path = require('path');

const sleep = time => new Promise(r => setTimeout(r, time));
jest.setTimeout(15000);

const app = new Application({
  path: electronPath,
  args: [path.join(__dirname, '../../dist/electron/main.js')],
});

beforeAll(async () => app.start());

afterAll(async () => {
  if (app && app.isRunning()) {
    return app.stop();
  }
  return undefined;
});

describe('Favourite queries', () => {
  test('initialize workbase', async () => {
    const count = await app.client.getWindowCount();
    assert.equal(count, 1);
  });

  // test('select keyspace', async () => {
  //   app.client.click('.keyspaces');
  //   await app.client.waitUntilWindowLoaded();

  //   const keyspaceList = app.client.selectByAttribute('class', 'keyspaces-list');
  //   assert.ok(keyspaceList);

  //   assert.equal(await app.client.getText('.keyspaces'), 'keyspace');

  //   app.client.click('#gene');

  //   assert.equal(await app.client.getText('.keyspaces'), 'gene');
  // });

  // test('right click on canvas', async () => {
  //   app.client.click('.CodeMirror');

  //   await sleep(1000);

  //   app.client.keys('match $x isa cousins; limit 1; get;');

  //   await sleep(1000);

  //   app.client.click('.run-btn');

  //   await sleep(2000);

  //   app.client.rightClick('#graph-div', 10, 10);

  //   await sleep(2000);

  //   assert.equal(await app.client.getText('#context-menu'), 'Delete\nExplain\nShortest Path');
  //   await sleep(2000);

  //   assert.equal(await app.client.getAttribute('.delete-nodes', 'class'), 'context-action delete-nodes disabled');
  //   await sleep(2000);

  //   assert.equal(await app.client.getAttribute('.explain-node', 'class'), 'context-action explain-node disabled');
  //   await sleep(2000);

  //   assert.equal(await app.client.getAttribute('.compute-shortest-path', 'class'), 'context-action compute-shortest-path disabled');
  //   await sleep(2000);
  // });

  // test('right click on node', async () => {
  //   app.client.rightClick('#graph-div', 540, 470);

  //   await sleep(2000);

  //   assert.equal(await app.client.getAttribute('.delete-nodes', 'class'), 'context-action delete-nodes');
  //   await sleep(2000);

  //   assert.equal(await app.client.getAttribute('.explain-node', 'class'), 'context-action explain-node disabled');
  //   await sleep(2000);

  //   assert.equal(await app.client.getAttribute('.compute-shortest-path', 'class'), 'context-action compute-shortest-path disabled');
  //   await sleep(2000);
  // });

  // test('right click on inferred node', async () => {
  //   app.client.rightClick('#graph-div', 750, 470);

  //   await sleep(2000);

  //   assert.equal(await app.client.getAttribute('.delete-nodes', 'class'), 'context-action delete-nodes');
  //   await sleep(2000);

  //   assert.equal(await app.client.getAttribute('.explain-node', 'class'), 'context-action explain-node');
  //   await sleep(2000);

  //   assert.equal(await app.client.getAttribute('.compute-shortest-path', 'class'), 'context-action compute-shortest-path disabled');
  //   await sleep(2000);
  // });
});
