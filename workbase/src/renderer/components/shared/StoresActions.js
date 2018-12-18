

// Schema Design store actions
export const DEFINE_ENTITY_TYPE = 'define-entity-type';
export const DEFINE_ATTRIBUTE_TYPE = 'define-attribute-type';
export const DEFINE_RELATIONSHIP_TYPE = 'define-relationship-type';
export const DEFINE_ROLE = 'define-role';
export const DELETE_TYPE = 'delete-type';
export const DELETE_SCHEMA_CONCEPT = 'delete-schema-concept';
export const DELETE_ATTRIBUTE = 'delete-attribute';
export const LOAD_SCHEMA = 'load-schema';
export const ADD_TYPE = 'add-type';
export const COMMIT_TX = 'commit-tx';
export const REFRESH_SELECTED_NODE = 'refresh-selected-node';
export const DELETE_PLAYS_ROLE = 'delete-plays-role';
export const DELETE_RELATES_ROLE = 'delete-relates-role';
export const DEFINE_RULE = 'define-rule';

// Visualiser store actions
export const RUN_CURRENT_QUERY = 'run-current-query';
export const EXPLAIN_CONCEPT = 'explain-concept';
export const UPDATE_NODES_LABEL = 'update-nodes-label';
export const UPDATE_NODES_COLOUR = 'update-nodes-colour';
export const DELETE_SELECTED_NODES = 'delete-selected-nodes';
export const LOAD_NEIGHBOURS = 'load-neighbours';
export const LOAD_ATTRIBUTES = 'load-attributes';
export const ADD_ATTRIBUTE_TYPE = 'add-attribute-types';


// Common actions shared by the two canvas stores (SchemaDesign && DataManagement)
export const UPDATE_METATYPE_INSTANCES = 'update-metatype-instances';
export const INITIALISE_VISUALISER = 'initialise-visualiser';
export const CANVAS_RESET = 'canvas-reset';
export const CURRENT_KEYSPACE_CHANGED = 'current-keyspace-changed';
export const OPEN_GRAKN_TX = 'open-grakn-tx';

