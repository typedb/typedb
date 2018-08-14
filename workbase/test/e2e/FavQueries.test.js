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

  test('select keyspace', async () => {
    app.client.click('#keyspaces');
    await app.client.waitUntilWindowLoaded();

    const keyspaceList = app.client.selectByAttribute('class', 'keyspaces-list');
    assert.ok(keyspaceList);

    assert.equal(await app.client.getText('#keyspaces'), 'keyspace');

    app.client.click('#gene');

    assert.equal(await app.client.getText('#keyspaces'), 'gene');
  });

  test('add favourite query', async () => {
    app.client.click('#types-panel');
    await app.client.waitUntilWindowLoaded();
    const typesPanel = app.client.selectByAttribute('class', 'types-panel');

    assert.ok(typesPanel);
    app.client.click('#entities');
    await sleep(1000);

    app.client.click('#add-query-btn').keys('fav-query').click('#save-fav-query');

    app.client.rightClick('#graph-div');
    await sleep(1000);
    app.client.click('#clear-graph');
    await sleep(1000);
    app.client.click('.confirm');
    await sleep(1000);

    app.client.click('#fav-queries-btn');
    await app.client.waitUntilWindowLoaded();

    await sleep(3000);

    const queryNameSaved = await app.client.getText('#list-key');
    await sleep(1000);
    assert.equal(queryNameSaved, 'fav-query');
  });

  test('use favourite query', async () => {
    const favQueriesList = await app.client.getHTML('#fav-queries-list');
    assert.ok(favQueriesList);

    let noOfNodes = await app.client.getText('#nodes');
    assert.equal(noOfNodes, 'nodes: 0');

    app.client.click('#use-btn');
    app.client.click('#run-query');
    await sleep(3000);

    noOfNodes = await app.client.getText('#nodes');
    assert.equal(noOfNodes, 'nodes: 2');
  });

  test('delete favourite query', async () => {
    app.client.click('#fav-queries-btn');
    await app.client.waitUntilWindowLoaded();

    app.client.click('#delete-btn');
    await sleep(2000);
    const noQueriesSaved = await app.client.getText('#no-saved');
    assert.equal(noQueriesSaved, 'no saved queries');
  });
});
