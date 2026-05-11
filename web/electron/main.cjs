const { app, BrowserWindow } = require('electron');
const path = require('path');
const { spawn } = require('child_process');

let bridgeProcess = null;

function spawnBridge(command, args) {
  try {
    const p = spawn(command, args, { stdio: 'ignore', windowsHide: true });
    p.on('error', () => {
      if (bridgeProcess === p) bridgeProcess = null;
    });
    p.on('exit', () => {
      if (bridgeProcess === p) bridgeProcess = null;
    });
    return p;
  } catch {
    return null;
  }
}

function startBridge() {
  if (bridgeProcess) return;
  const isPackaged = app.isPackaged;
  const bridgeExe = isPackaged ? path.join(process.resourcesPath, 'bridge', 'realtime_bridge.exe') : '';
  const bridgePy = isPackaged ? path.join(process.resourcesPath, 'bridge', 'realtime_bridge.py') : path.resolve(__dirname, '..', '..', 'realtime_bridge.py');

  bridgeProcess =
    (bridgeExe ? spawnBridge(bridgeExe, []) : null) ||
    spawnBridge('python', [bridgePy]) ||
    spawnBridge('py', ['-3', bridgePy]) ||
    null;
}

function stopBridge() {
  if (!bridgeProcess) return;
  try {
    bridgeProcess.kill();
  } catch {
    void 0;
  }
  bridgeProcess = null;
}

function createWindow() {
  const win = new BrowserWindow({
    width: 1280,
    height: 800,
    backgroundColor: '#09090b',
    webPreferences: {
      contextIsolation: true,
      sandbox: true,
    },
  });

  const appHtmlPath = app.isPackaged
    ? path.join(process.resourcesPath, 'app', 'index.html')
    : path.resolve(__dirname, '..', '..', 'index.html');
  win.loadFile(appHtmlPath);
}

app.whenReady().then(() => {
  startBridge();
  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  stopBridge();
  if (process.platform !== 'darwin') app.quit();
});
