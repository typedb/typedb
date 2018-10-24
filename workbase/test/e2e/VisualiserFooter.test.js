const Application = require('spectron').Application;
const assert = require('assert');
const electronPath = require('electron'); // Require Electron from the binaries included in node_modules.
const path = require('path');

const sleep = time => new Promise(r => setTimeout(r, time));
jest.setTimeout(30000);

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

describe('Canvas Data', () => {
  test('initialize workbase', async () => {
    const count = await app.client.getWindowCount();
    assert.equal(count, 1);
  });

  test('select keyspace', async () => {
    app.client.click('.keyspaces');
    await app.client.waitUntilWindowLoaded();

    const keyspaceList = app.client.selectByAttribute('class', 'keyspaces-list');
    assert.ok(keyspaceList);

    assert.equal(await app.client.getText('.keyspaces'), 'keyspace');

    app.client.click('#gene');

    assert.equal(await app.client.getText('.keyspaces'), 'gene');
  });

  test('entity', async () => {
    await app.client.click('.CodeMirror');

    await app.client.keys('match $x isa person; limit 1; get;');

    await app.client.click('.run-btn');

    await app.client.waitUntil(async () => (await app.client.getText('.no-of-entities')) !== 'entities: 0', 25000, 'wait for canvas data to be updated');

    const noOfEntities = await app.client.getText('.no-of-entities');
    assert.equal(noOfEntities, 'entities: 1');

    await app.client.click('.clear-editor');
  });
  test('attribute', async () => {
    await app.client.click('.CodeMirror');

    await app.client.keys('match $x isa age; limit 1; get;');

    await app.client.click('.run-btn');

    await app.client.waitUntil(async () => (await app.client.getText('.no-of-attributes')) !== 'attributes: 0', 25000, 'wait for canvas data to be updated');

    const noOfAttributes = await app.client.getText('.no-of-attributes');
    assert.equal(noOfAttributes, 'attributes: 1');

    await app.client.click('.clear-editor');
  });
  test('relationship', async () => {
    await app.client.click('.CodeMirror');

    await app.client.keys('match $x isa parentship; limit 1; get;');

    await app.client.click('.run-btn');

    await app.client.waitUntil(async () => (await app.client.getText('.no-of-relationships')) !== 'relationships: 0', 25000, 'wait for canvas data to be updated');

    const noOfRelationships = await app.client.getText('.no-of-relationships');
    assert.equal(noOfRelationships, 'relationships: 1');

    await app.client.click('.clear-editor');
  });
});
