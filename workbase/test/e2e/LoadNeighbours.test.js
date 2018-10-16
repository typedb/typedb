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

describe('Load neighbours', () => {
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

  // test('double click on type', async () => {
  //   app.client.click('.CodeMirror');

  //   await sleep(1000);

  //   app.client.keys('match $x sub entity; offset 1; limit 1; get;');

  //   await sleep(1000);

  //   app.client.click('.run-btn');

  //   await sleep(1000);

  //   app.client.leftClick('#graph-div');
  //   app.client.leftClick('#graph-div');

  //   await sleep(4000);

  //   const noOfEntities = await app.client.getText('.no-of-entities');
  //   assert.equal(noOfEntities, 'entities: 20');
  //   const noOfAttributes = await app.client.getText('.no-of-attributes');
  //   assert.equal(noOfAttributes, 'attributes: 0');
  //   const noOfRelationships = await app.client.getText('.no-of-relationships');
  //   assert.equal(noOfRelationships, 'relationships: 0');

  //   app.client.click('.clear-graph-btn');
  //   app.client.click('.clear-editor');
  // });

  // test('double click on entity', async () => {
  //   app.client.click('.CodeMirror');

  //   await sleep(1000);

  //   app.client.keys('match $x isa person; limit 1; get;');

  //   await sleep(1000);

  //   app.client.click('.run-btn');

  //   await sleep(1000);

  //   app.client.leftClick('#graph-div');
  //   app.client.leftClick('#graph-div');

  //   await sleep(6000);

  //   const noOfEntities = await app.client.getText('.no-of-entities');
  //   assert.equal(noOfEntities, 'entities: 10');
  //   const noOfAttributes = await app.client.getText('.no-of-attributes');
  //   assert.equal(noOfAttributes, 'attributes: 0');
  //   const noOfRelationships = await app.client.getText('.no-of-relationships');
  //   assert.equal(noOfRelationships, 'relationships: 14');

  //   app.client.click('.clear-graph-btn');
  //   app.client.click('.clear-editor');
  // });

  // test('double click on attribute', async () => {
  //   app.client.click('.CodeMirror');

  //   await sleep(1000);

  //   app.client.keys('match $x isa age; limit 1; get;');

  //   await sleep(1000);

  //   app.client.click('.run-btn');

  //   await sleep(1000);

  //   app.client.leftClick('#graph-div');
  //   app.client.leftClick('#graph-div');

  //   await sleep(6000);

  //   const noOfEntities = await app.client.getText('.no-of-entities');
  //   assert.equal(noOfEntities, 'entities: 1');
  //   const noOfAttributes = await app.client.getText('.no-of-attributes');
  //   assert.equal(noOfAttributes, 'attributes: 1');
  //   const noOfRelationships = await app.client.getText('.no-of-relationships');
  //   assert.equal(noOfRelationships, 'relationships: 0');

  //   app.client.click('.clear-graph-btn');
  //   app.client.click('.clear-editor');
  // });

  // test('double click on relationship', async () => {
  //   app.client.click('.CodeMirror');

  //   await sleep(1000);

  //   app.client.keys('match $x isa parentship; limit 1; get;');

  //   await sleep(1000);

  //   app.client.click('.run-btn');

  //   await sleep(1000);

  //   app.client.leftClick('#graph-div');
  //   app.client.leftClick('#graph-div');

  //   await sleep(6000);

  //   const noOfEntities = await app.client.getText('.no-of-entities');
  //   assert.equal(noOfEntities, 'entities: 2');
  //   const noOfAttributes = await app.client.getText('.no-of-attributes');
  //   assert.equal(noOfAttributes, 'attributes: 0');
  //   const noOfRelationships = await app.client.getText('.no-of-relationships');
  //   assert.equal(noOfRelationships, 'relationships: 1');
  // });
});
