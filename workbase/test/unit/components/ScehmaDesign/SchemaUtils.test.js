import { computeAttributes } from '@/components/SchemaDesign/SchemaUtils.js';
import MockConcepts from '../../../helpers/MockConcepts';


jest.mock('@/components/shared/PersistentStorage', () => ({
}));

describe('Schema Utils', () => {
  test('Compute Attributes', async () => {
    const nodes = await computeAttributes([MockConcepts.getMockEntityType()]);
    expect(nodes[0].attributes[0].type).toBe('name');
    expect(nodes[0].attributes[0].dataType).toBe('String');
  });
});
