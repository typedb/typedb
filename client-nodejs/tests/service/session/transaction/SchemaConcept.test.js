const env = require('../../../support/GraknTestEnvironment');
let session;
let tx;

beforeAll(() => {
    session = env.session();
});

afterAll(async () => {
    await env.tearDown();
});

beforeEach(async () => {
    tx = await session.transaction(env.txType().WRITE);
})

afterEach(() => {
    tx.close();
});


describe("Schema concept methods", () => {

    test("Get/set label", async () => {
        await tx.query('define person sub entity;')
        const personSchemaConcept = await tx.getSchemaConcept('person');
        expect(await personSchemaConcept.label()).toBe('person');
        await personSchemaConcept.label('superperson');
        const superPersonSchemaConcept = await tx.getSchemaConcept('superperson');
        expect(await superPersonSchemaConcept.label()).toBe('superperson');
        const nullSchemaConcept = await tx.getSchemaConcept('person');
        expect(nullSchemaConcept).toBeNull();
    });

    test("isImplicit", async () => {
        await tx.query('define person sub entity;')
        const personSchemaConcept = await tx.getSchemaConcept('person');
        expect(await personSchemaConcept.isImplicit()).toBe(false);
        await tx.query('define person has name; name sub attribute, datatype string;');
        const implicitSchemaConcept = await tx.getSchemaConcept('@has-name');
        expect(await implicitSchemaConcept.isImplicit()).toBe(true);
    });

    test("Get sups and subs", async () => {
        await tx.query('define person sub entity;')
        const personSchemaConcept = await tx.getSchemaConcept('person');
        const sups = await (await personSchemaConcept.sups()).collect();
        expect(sups.length).toBe(2);
        const label1 = await sups[0].label();
        const label2 = await sups[1].label();
        const supLabels = [label1, label2];
        supLabels.sort();
        expect(supLabels[0]).toBe('entity');
        expect(supLabels[1]).toBe('person');
        const entitySchemaConcept = await tx.getSchemaConcept('entity');
        const entitySubs = await (await entitySchemaConcept.subs()).collect();
        expect(entitySubs.length).toBe(2);
        const subLabel1 = await entitySubs[0].label();
        const subLabel2 = await entitySubs[1].label();
        const subLabels = [subLabel1, subLabel2];
        subLabels.sort();
        expect(subLabels[0]).toBe('entity');
        expect(subLabels[1]).toBe('person');
    });

    test("Get sup", async () => {
        const entitySchemaConcept = await tx.getSchemaConcept('entity');
        const metaType = await entitySchemaConcept.sup();
        expect(metaType.baseType).toBe('META_TYPE');
        const sup = await metaType.sup();
        expect(sup).toBeNull();
    });

    test("Set sup", async () => {
        const humanSchemaConcept = await tx.putEntityType('human');
        const maleSchemaConcept = await tx.putEntityType('male');
        const supType = await maleSchemaConcept.sup();
        expect(await supType.label()).toBe('entity');
        await maleSchemaConcept.sup(humanSchemaConcept);
        const sup = await maleSchemaConcept.sup();
        expect(await sup.label()).toBe('human');
    });
})

