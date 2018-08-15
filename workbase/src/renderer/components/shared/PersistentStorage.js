import Storage from 'electron-store';
import { version } from '../../../../package.json';

// Config file in Mac system in: ~/Library/Application Support/grakn-workbase/config.json
const storage = new Storage();

// Store current project version in persistent storage
// (to allow migrations and avoid conflicts in future)
const storedVersion = storage.get('project-version');
if (!storedVersion) storage.set('project-version', version);

export default storage;
