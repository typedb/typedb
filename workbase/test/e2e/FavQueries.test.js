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

describe('Favourite queries', () => {
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

  test('add new favourite query', async () => {
    app.client.click('.CodeMirror');

    await sleep(1000);

    app.client.keys('match $x isa person; get;');

    app.client.click('.add-fav-query-btn');

    await sleep(1000);

    app.client.click('.save-query-btn');

    await sleep(1000);

    assert.equal(await app.client.getText('.fav-query-name-tooltip'), 'Please write a query name');

    app.client.click('.query-name-input');

    await sleep(1000);

    app.client.keys('get persons');

    app.client.click('.save-query-btn');

    await sleep(1000);

    assert.equal(await app.client.getText('.toasted'), 'New query saved!\nCLOSE');

    await sleep(1000);

    app.client.click('.close-add-fav-query-container');
    app.client.click('.action');
  });

  test('add existing favourite query', async () => {
    app.client.click('.add-fav-query-btn');

    await sleep(1000);

    app.client.click('.query-name-input');

    await sleep(1000);

    app.client.keys('get persons');

    await sleep(1000);

    app.client.click('.save-query-btn');

    await sleep(1000);

    assert.equal(await app.client.getText('.toasted'), 'Query name already saved. Please choose a different name.\nCLOSE');
  });


  test('run favourite query', async () => {
    app.client.click('.fav-queries-container-btn');

    await sleep(1000);

    app.client.click('.run-fav-query-btn');

    await sleep(1000);

    app.client.click('.run-btn');

    await sleep(1000);

    const noOfEntities = await app.client.getText('.no-of-entities');
    await sleep(1000);

    assert.equal(noOfEntities, 'entities: 30');

    await app.client.click('.clear-graph-btn');
  });

  test('edit favourite query', async () => {
    await app.client.click('.fav-queries-container-btn');

    await app.client.click('.edit-fav-query-btn');

    await app.client.click('.CodeMirror-focused');

    await app.client.keys(['ArrowLeft', 'ArrowLeft', 'ArrowLeft', 'ArrowLeft', 'limit 1; ']);

    await app.client.click('.save-edited-fav-query');

    await app.client.click('.action');

    await app.client.click('.run-fav-query-btn');

    const noOfEntities = await app.client.getText('.no-of-entities');

    await assert.equal(noOfEntities, 'entities: 0');

    await assert.equal((await app.client.getText('.CodeMirror'))[0], ' match $x isa person; limit 1; get;');
    await assert.equal((await app.client.getText('.CodeMirror'))[1], 'match $x isa person; limit 1; get;');
  });

  test('delete favourite query', async () => {
    await sleep(5000);

    await app.client.click('.delete-fav-query-btn');

    await assert.equal(await app.client.getText('.toasted'), 'Query get persons has been deleted from saved queries.\nCLOSE');

    await sleep(1000);
  });
});
