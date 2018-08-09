import { shallowMount } from '@vue/test-utils';
import GraqlEditor from '@/components/DataManagement/MenuBar/GraqlEditor/GraqlEditor';
import { RUN_CURRENT_QUERY, CANVAS_RESET } from '@/components/shared/StoresActions';

jest.mock('@/components/shared/PersistentStorage', () => ({}));
jest.mock('@/components/DataManagement/MenuBar/GraqlEditor/GraqlCodeMirror', () => ({
  getCodeMirror: () => ({
    on: () => {},
    setValue: jest.fn(),
    setOption: jest.fn(),
  }),
  createGraqlEditorHistory: () => ({
    on: () => {},
    addToHistory: jest.fn(),
  }),
}));


let vm;
let wrapper;
let localStore;

beforeEach(() => {
  localStore = {
    currentQuery: '',
    currentKeyspace: 'mock-kepspace',
    getCurrentKeyspace() { return this.currentKeyspace; },
    getCurrentQuery() { return this.currentQuery; },
    setCurrentQuery(val) { this.currentQuery = val; },
    dispatch: jest.fn().mockImplementation(() => new Promise((() => {}))),
  };
  wrapper = shallowMount(GraqlEditor, { propsData: { localStore } });
  vm = wrapper.vm;
});

describe('GraqlEditor.vue', () => {
  // TODO finish this
//   test('When currentQuery changes in localStore, update the codemirror query', () => {
//     localStore.setCurrentQuery('update new current query');
//     vm.$nextTick(() => {
//       expect(vm.$data.codeMirror.setValue).toBeCalledWith('update new current query');
//     });
//   });

  test('When click on clearButton set query to empty string', () => {
    wrapper.find({ ref: 'clearButton' }).trigger('click');
    expect(vm.$data.codeMirror.setValue).toBeCalledWith('');
  });

  describe('Dispatch Actions on user interaction', () => {
    test('When runQueryButton is clicked: dispatch "run-query" and set showType to false', () => {
      wrapper.setData({ showTypes: true });
      wrapper.find({ ref: 'runQueryButton' }).trigger('click');
      expect(localStore.dispatch).toBeCalledWith(RUN_CURRENT_QUERY);
      expect(vm.$data.showTypes).toBeFalsy();
    });

    test('When type-query event emitted set query to match isa type', () => {
      vm.$emit('type-selected', 'mock-type');

      expect(vm.$data.codeMirror.setValue).toBeCalledWith('match $x isa mock-type; get;');
      expect(localStore.dispatch).toBeCalledWith(RUN_CURRENT_QUERY);
    });

    test('When load-schema event emitted, emit run query with "match sub type" and loadingSchema to true', () => {
      vm.$emit('meta-type-selected', 'mock-meta-type');

      expect(vm.$data.codeMirror.setValue).toBeCalledWith('match $x sub mock-meta-type; get;');
      expect(localStore.dispatch).toBeCalledWith(RUN_CURRENT_QUERY);
    });
  });
});
