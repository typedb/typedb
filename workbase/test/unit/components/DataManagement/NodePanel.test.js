import { mount } from '@vue/test-utils';
import NodePanel from '@/components/DataManagement/DataManagementContent/NodePanel';

describe('NodePanel.vue', () => {
  test('When selectedNode is SchemaConcept, empty attributes', async () => {
    const localStore = {
      getSelectedNode() { },
      getSelectedNodes() { },
      getNode() { return { isSchemaConcept: () => true }; },
    };
    const vm = mount(NodePanel, { propsData: { localStore } }).vm;
    expect(vm.attributes).toHaveLength(0);
    // Trigger watcher node()
    await vm.$options.watch.node.call(vm, [{ id: '123' }]);
    expect(vm.attributes).toHaveLength(0);
  });

  function getMockNode() {
    const attribute1 = {
      type: () => Promise.resolve({ label: () => Promise.resolve('name') }),
      value: () => Promise.resolve('Demeclezio'),
    };
    const attribute2 = {
      type: () => Promise.resolve({ label: () => Promise.resolve('age') }),
      value: () => Promise.resolve(345678), // Demeclezio is clearly a very old person
    };
    return { isSchemaConcept: () => false, attributes: () => Promise.resolve({ collect: () => Promise.resolve([attribute1, attribute2]) }) };
  }

  test('When selectedNode is not SchemaConcept, attributes computed correctly', async () => {
    // No node currently selected
    const localStore = { getSelectedNode: () => { }, getSelectedNodes: () => { }, getNode: () => getMockNode() };
    const vm = mount(NodePanel, { propsData: { localStore } }).vm;
    expect(vm.attributes).toHaveLength(0);
    // Simulate selecting a new node by triggering watcher node()
    await vm.$options.watch.node.call(vm, [{ id: '123' }]);
    expect(vm.attributes).toHaveLength(2);
    // Check sorted correctly by type label value and check values
    expect(vm.attributes[0].type).toBe('age');
    expect(vm.attributes[0].value).toBe(345678);
    expect(vm.attributes[1].type).toBe('name');
    expect(vm.attributes[1].value).toBe('Demeclezio');
  });

  test('When node gets selected, showNodePanel is true, when deselect it becomes false', async () => {
    const localStore = { getSelectedNode: () => ({ id: '123', baseType: 'entity' }), getSelectedNodes: () => ([{ id: '123', baseType: 'entity' }]), getNode: () => getMockNode() };
    const vm = mount(NodePanel, { propsData: { localStore } }).vm;
    // Simulate selecting a new node by triggering watcher node()
    await vm.$options.watch.node.call(vm, [{ id: '123' }]);
    expect(vm.$data.showNodePanel).toBeTruthy();
    // Simulate deselecting node
    await vm.$options.watch.node.call(vm, undefined);
    expect(vm.$data.showNodePanel).toBeFalsy();
  });
});
