// const Application = require('spectron').Application;
// const assert = require('assert');
// const electronPath = require('electron'); // Require Electron from the binaries included in node_modules.
// const path = require('path');
//
// const sleep = time => new Promise(r => setTimeout(r, time));
// jest.setTimeout(15000);
//
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
//     return app.stop();
//   }
//   return undefined;
// });
//
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
//     await sleep(1000);
//
//     assert.equal(await app.client.getText('#keyspaces'), 'gene');
//     await sleep(1000);
//   });
//
//   test('load entity types', async () => {
//     app.client.click('#types-panel');
//     await app.client.waitUntilWindowLoaded();
//     const typesPanel = app.client.selectByAttribute('class', 'types-panel');
//
//     assert.ok(typesPanel);
//     await sleep(1000);
//
//     let noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 0');
//
//     app.client.click('#entities');
//     await sleep(3000);
//
//     noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 2');
//
//     app.client.rightClick('#graph-div');
//     await sleep(1000);
//     app.client.click('#clear-graph');
//     await sleep(1000);
//     app.client.click('.confirm');
//     await sleep(1000);
//
//     noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 0');
//   });
//
//   test('load attribute types', async () => {
//     app.client.click('#types-panel');
//     await app.client.waitUntilWindowLoaded();
//     const typesPanel = app.client.selectByAttribute('class', 'types-panel');
//
//     assert.ok(typesPanel);
//     await sleep(1000);
//
//     let noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 0');
//
//     app.client.click('#attributes');
//     await sleep(3000);
//
//     noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 10');
//
//     app.client.rightClick('#graph-div');
//     await sleep(1000);
//     app.client.click('#clear-graph');
//     await sleep(1000);
//     app.client.click('.confirm');
//     await sleep(1000);
//
//     noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 0');
//   });
//
//   test('load relationship types', async () => {
//     app.client.click('#types-panel');
//     await app.client.waitUntilWindowLoaded();
//     const typesPanel = app.client.selectByAttribute('class', 'types-panel');
//
//     assert.ok(typesPanel);
//     await sleep(1000);
//
//     let noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 0');
//
//     app.client.click('#relationships');
//     await sleep(3000);
//
//     noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 7');
//
//     app.client.rightClick('#graph-div');
//     await sleep(1000);
//     app.client.click('#clear-graph');
//     await sleep(1000);
//     app.client.click('.confirm');
//     await sleep(1000);
//
//     noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 0');
//   });
//
//   test('load person entities', async () => {
//     app.client.click('#types-panel');
//     await app.client.waitUntilWindowLoaded();
//     const typesPanel = app.client.selectByAttribute('class', 'types-panel');
//
//     assert.ok(typesPanel);
//     await sleep(1000);
//
//     app.client.click('#list-entities');
//     await app.client.waitUntilWindowLoaded();
//     const entitiesTab = await app.client.getHTML('#entities-tab');
//
//     assert.ok(entitiesTab);
//     await sleep(3000);
//
//     let noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 0');
//
//     app.client.click('#person-btn');
//     await sleep(1000);
//
//     noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 30');
//
//     app.client.rightClick('#graph-div');
//     await sleep(1000);
//     app.client.click('#clear-graph');
//     await sleep(1000);
//     app.client.click('.confirm');
//     await sleep(1000);
//
//     noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 0');
//   });
//
//   test('load age attributes', async () => {
//     app.client.click('#types-panel');
//     await app.client.waitUntilWindowLoaded();
//     const typesPanel = app.client.selectByAttribute('class', 'types-panel');
//
//     assert.ok(typesPanel);
//     await sleep(1000);
//
//     app.client.click('#list-attributes');
//     await app.client.waitUntilWindowLoaded();
//     const entitiesTab = await app.client.getHTML('#attributes-tab');
//
//     assert.ok(entitiesTab);
//     await sleep(3000);
//
//     let noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 0');
//
//     app.client.click('#age-btn');
//     await sleep(1000);
//
//     noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 18');
//
//     app.client.rightClick('#graph-div');
//     await sleep(1000);
//     app.client.click('#clear-graph');
//     await sleep(1000);
//     app.client.click('.confirm');
//     await sleep(1000);
//
//     noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 0');
//   });
//
//   test('load parentiship relationships', async () => {
//     app.client.click('#types-panel');
//     await app.client.waitUntilWindowLoaded();
//     const typesPanel = app.client.selectByAttribute('class', 'types-panel');
//
//     assert.ok(typesPanel);
//     await sleep(1000);
//
//     app.client.click('#list-relationships');
//     await app.client.waitUntilWindowLoaded();
//     const entitiesTab = await app.client.getHTML('#relationships-tab');
//
//     assert.ok(entitiesTab);
//     await sleep(4000);
//
//     let noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 0');
//     let noOfEdges = await app.client.getText('#edges');
//     assert.equal(noOfEdges, 'edges: 0');
//
//     await app.client.click('#marriage-btn');
//     await sleep(6000);
//
//     noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 74');
//     noOfEdges = await app.client.getText('#edges');
//     assert.equal(noOfEdges, 'edges: 60');
//
//     app.client.rightClick('#graph-div');
//     await sleep(1000);
//     app.client.click('#clear-graph');
//     await sleep(1000);
//     app.client.click('.confirm');
//     await sleep(1000);
//
//     noOfNodes = await app.client.getText('#nodes');
//     assert.equal(noOfNodes, 'nodes: 0');
//     noOfEdges = await app.client.getText('#edges');
//     assert.equal(noOfEdges, 'edges: 0');
//   });
});
