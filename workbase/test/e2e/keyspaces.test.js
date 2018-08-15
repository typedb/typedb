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

  test('create new keyspace', async () => {
    app.client.click('#manage-keyspaces');
    await sleep(1000);
    app.client.click('#create-keyspace-btn');
    await sleep(1000);
    await app.client.setValue('#keyspace-name', 'test');
    app.client.click('#create-btn');

    await sleep(7000);

    assert.equal(await app.client.getText('.toasted.primary.default'), 'New keyspace [test] successfully created!\nCLOSE');
  });

  test('delete exisiting keyspace', async () => {
    app.client.click('#delete-test');
    await sleep(1000);
    app.client.click('.confirm');
    await sleep(3000);
    assert.equal(await app.client.getText('.toasted.primary.default'), 'Keyspace [test] successfully deleted\nCLOSE');
  });
});
