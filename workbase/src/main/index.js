import { app, BrowserWindow, Menu } from 'electron';

const isProd = (process.env.NODE_ENV !== 'development');
if (!isProd) {
  app.commandLine.appendSwitch('remote-debugging-port', '8315');
}

/**
 * Set `__static` path to static files in production
 * https://simulatedgreg.gitbooks.io/electron-vue/content/en/using-static-assets.html
 */
if (isProd) {
  global.__static = require('path').join(__dirname, '/static').replace(/\\/g, '\\\\') // eslint-disable-line
}

let mainWindow;
// Use webpack dev server URL if in development mode
const winURL = isProd
  ? `file://${__dirname}/index.html`
  : 'http://127.0.0.1:9080/index.html';


function buildApplicationMenu() {
  const applicationSubmenu = [];
  // The about window only works on Mac out-of-the-box - will need to implement custom for Windows/Linux
  if (process.platform === 'darwin') {
    applicationSubmenu.push({ label: 'About Grakn Workbase', selector: 'orderFrontStandardAboutPanel:' });
    applicationSubmenu.push({ type: 'separator' });
  }
  applicationSubmenu.push({ label: 'Quit', accelerator: 'Command+Q', click() { app.quit(); } });
  // Create the Application's main menu
  const menuTemplate = [{
    label: 'Application',
    submenu: applicationSubmenu,
  }, {
    label: 'Edit',
    submenu: [
      { label: 'Undo', accelerator: 'CmdOrCtrl+Z', selector: 'undo:' },
      { label: 'Redo', accelerator: 'Shift+CmdOrCtrl+Z', selector: 'redo:' },
      { type: 'separator' },
      { label: 'Cut', accelerator: 'CmdOrCtrl+X', selector: 'cut:' },
      { label: 'Copy', accelerator: 'CmdOrCtrl+C', selector: 'copy:' },
      { label: 'Paste', accelerator: 'CmdOrCtrl+V', selector: 'paste:' },
      { label: 'Select All', accelerator: 'CmdOrCtrl+A', selector: 'selectAll:' },
    ] },
  {
    label: 'View',
    submenu: [
      {
        role: 'reload',
      }],
  },
  ];

  Menu.setApplicationMenu(Menu.buildFromTemplate(menuTemplate));
}

function createWindow() {
  /**
   * Initial window options
   */
  mainWindow = new BrowserWindow({
    height: 1000,
    useContentSize: true,
    width: 1600,
    icon: `${__static}/img/icon.png`,
    titleBarStyle: 'hidden',
    darkTheme: true,
    minWidth: 1100,
    minHeight: 700,
    // Set webSecurity to false in development mode to avoid AJAX calls blocked due to CORS (due to WebapckDevServer).
    webPreferences: { webSecurity: isProd },
  });

  mainWindow.loadURL(winURL);

  mainWindow.on('closed', () => {
    mainWindow = null;
  });

  if (isProd) buildApplicationMenu();
}

app.on('ready', createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('activate', () => {
  if (mainWindow === null) {
    createWindow();
  }
});

/**
 * Auto Updater
 *
 * Uncomment the following code below and install `electron-updater` to
 * support auto updating. Code Signing with a valid certificate is required.
 * https://simulatedgreg.gitbooks.io/electron-vue/content/en/using-electron-builder.html#auto-updating
 */

/*
import { autoUpdater } from 'electron-updater'

autoUpdater.on('update-downloaded', () => {
  autoUpdater.quitAndInstall()
})

app.on('ready', () => {
  if (process.env.NODE_ENV === 'production') autoUpdater.checkForUpdates()
})
 */
