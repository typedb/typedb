// const Application = require('spectron').Application;
// const assert = require('assert');
// const electronPath = require('electron'); // Require Electron from the binaries included in node_modules.
// const path = require('path');
//
// const sleep = time => new Promise(r => setTimeout(r, time));
// jest.setTimeout(15000);
//
// const app = new Application({
//   path: electronPath,
//   args: [path.join(__dirname, '../../dist/electron/main.js')],
// });
//
// beforeAll(async () => app.start());
//
// afterAll(async () => {
//   if (app && app.isRunning()) {
//     app.client.click('#settings');
//     await sleep(1000);
//     app.client.click('#Preferences');
//     await sleep(1000);
//     app.client.click('#clear-preferences');
//     await sleep(1000);
//     app.client.click('.confirm');
//     await sleep(1000);
//
//     return app.stop();
//   }
//   return undefined;
// });
//
describe('Types Panel', () => {
  test('initialize workbase', async () => {
    // const count = await app.client.getWindowCount();
    // assert.equal(count, 1);
  });
//
//   test('select keyspace', async () => {
//     app.client.click('#keyspaces');
//     await app.client.waitUntilWindowLoaded();
//
//     const keyspaceList = app.client.selectByAttribute('class', 'keyspaces-list');
//     assert.ok(keyspaceList);
//
//     assert.equal(await app.client.getText('#keyspaces'), 'keyspace');
//     await sleep(1000);
//
//     app.client.click('#gene');
//
//     assert.equal(await app.client.getText('#keyspaces'), 'gene');
//     await sleep(1000);
//   });
//
//   test('set query limit', async () => {
//     app.client.click('#cog');
//
//     const entitiesTab = await app.client.getHTML('#query-settings');
//     assert.ok(entitiesTab);
//
//     await app.client.setValue('#limit-query', '1');
//
//     app.client.click('#types-panel');
//     await app.client.waitUntilWindowLoaded();
//     const typesPanel = app.client.selectByAttribute('class', 'types-panel');
//     assert.ok(typesPanel);
//     app.client.click('#entities');
//     await sleep(1000);
//
//     const noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 1');
//
//     app.client.rightClick('#graph-div');
//     await sleep(1000);
//     app.client.click('#clear-graph');
//     await sleep(1000);
//     app.client.click('.confirm');
//     await sleep(1000);
//   });
//
//   test('set neighbours limit', async () => {
//     app.client.click('#cog');
//
//     const entitiesTab = await app.client.getHTML('#query-settings');
//     assert.ok(entitiesTab);
//
//     await app.client.setValue('#limit-neighbours', '1');
//
//     app.client.click('#types-panel');
//     await app.client.waitUntilWindowLoaded();
//
//     const typesPanel = app.client.selectByAttribute('class', 'types-panel');
//     assert.ok(typesPanel);
//     await sleep(1000);
//
//     app.client.click('#list-relationships');
//     await app.client.waitUntilWindowLoaded();
//     app.client.click('#parentship-btn');
//     await sleep(3000);
//
//     const noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 2');
//     const noOfEdges = await app.client.getText('#edges');
//     assert.equal(noOfEdges, 'edges: 1');
//
//     app.client.rightClick('#graph-div');
//     await sleep(1000);
//     app.client.click('#clear-graph');
//     await sleep(1000);
//     app.client.click('.confirm');
//     await sleep(1000);
//   });
//
//   test('dont auto load neighbours', async () => {
//     app.client.click('#cog');
//
//     const entitiesTab = await app.client.getHTML('#query-settings');
//     assert.ok(entitiesTab);
//
//     await app.client.click('#load-role-players');
//
//     app.client.click('#types-panel');
//     await app.client.waitUntilWindowLoaded();
//
//     const typesPanel = app.client.selectByAttribute('class', 'types-panel');
//     assert.ok(typesPanel);
//     await sleep(1000);
//
//     app.client.click('#list-relationships');
//     await app.client.waitUntilWindowLoaded();
//     app.client.click('#parentship-btn');
//     await sleep(3000);
//
//     const noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 1');
//     const noOfEdges = await app.client.getText('#edges');
//     assert.equal(noOfEdges, 'edges: 0');
//   });
});
