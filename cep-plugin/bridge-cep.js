/**
 * MCP Premiere Pro Bridge (CEP)
 * Uses CSInterface.evalScript to run ExtendScript in Premiere Pro.
 * Supports both file-based polling and WebSocket server for remote connections.
 *
 * Transport is selected in the panel UI:
 *   - File mode:   original polling of /tmp/premiere-mcp-bridge
 *   - WebSocket:   starts a WS server for direct remote MCP connections
 */

(function() {
    var fs = require('fs');
    var path = require('path');
    var os = require('os');

    // ─── WebSocket Server (loaded dynamically) ──────────────────────
    var WebSocketServer = null;
    try {
        var wsPath = path.join(__dirname, 'node_modules', 'ws');
        WebSocketServer = require(wsPath).Server;
    } catch (e) {
        try {
            WebSocketServer = require('ws').Server;
        } catch (e2) {
            // WebSocket not available — file-only mode
        }
    }

    var EXTENDSCRIPT_COMPAT_HELPERS = [
        'function __mcpEscapeString(value) {',
        '    return String(value)',
        '        .replace(/\\\\/g, "\\\\\\\\")',
        "        .replace(/\"/g, '\\\\\"')",
        '        .replace(/\\r/g, "\\\\r")',
        '        .replace(/\\n/g, "\\\\n")',
        '        .replace(/\\t/g, "\\\\t");',
        '}',
        'function __mcpStringify(value) {',
        '    if (value === null) return "null";',
        '    var valueType = typeof value;',
        '    if (valueType === "string") return "\\"" + __mcpEscapeString(value) + "\\"";',
        '    if (valueType === "number") return isFinite(value) ? String(value) : "null";',
        '    if (valueType === "boolean") return value ? "true" : "false";',
        '    if (value instanceof Array) {',
        '        var arrayParts = [];',
        '        for (var i = 0; i < value.length; i++) {',
        '            arrayParts.push(__mcpStringify(value[i]));',
        '        }',
        '        return "[" + arrayParts.join(",") + "]";',
        '    }',
        '    if (valueType === "object") {',
        '        var objectParts = [];',
        '        for (var key in value) {',
        '            if (value.hasOwnProperty && !value.hasOwnProperty(key)) continue;',
        '            if (typeof value[key] === "undefined" || typeof value[key] === "function") continue;',
        '            objectParts.push(__mcpStringify(String(key)) + ":" + __mcpStringify(value[key]));',
        '        }',
        '        return "{" + objectParts.join(",") + "}";',
        '    }',
        '    return "null";',
        '}',
        'if (typeof JSON === "undefined") { JSON = {}; }',
        'if (typeof JSON.stringify !== "function") { JSON.stringify = __mcpStringify; }'
    ].join('\n');

    function getDefaultTempPath() {
        var base = (os.platform() === 'win32') ? (process.env.TEMP || 'C:\\Temp') : '/tmp';
        return path.join(base, 'premiere-mcp-bridge');
    }

    function MCPPremiereBridge() {
        this.isConnected = false;
        this.tempDirectory = '';
        this.commandQueue = [];
        this.isProcessing = false;
        this.csInterface = new CSInterface();

        // WebSocket state
        this.wss = null;
        this.wsClients = [];
        this.wsPort = 9876;

        this.init();
    }

    MCPPremiereBridge.prototype.normalizeHostEnvironment = function(hostEnv) {
        if (!hostEnv) return null;
        if (typeof hostEnv === 'string') {
            return JSON.parse(hostEnv);
        }
        return hostEnv;
    };

    MCPPremiereBridge.prototype.init = function() {
        this.log('Initializing MCP Bridge (CEP)...', 'info');

        try {
            var env = this.normalizeHostEnvironment(this.csInterface.getHostEnvironment());
            if (env) {
                this.log('Premiere Pro version: ' + env.appVersion + ' (build ' + env.appId + ')', 'info');
            }
        } catch (e) {
            this.log('Warning: Could not get host environment: ' + e.message, 'warning');
        }

        this.loadConfig();
        this.updateUI();

        if (WebSocketServer) {
            this.log('WebSocket module available — can enable remote connections', 'info');
        } else {
            this.log('WebSocket module not found — file-only mode', 'warning');
        }
    };

    MCPPremiereBridge.prototype.getTempDirectory = function() {
        if (this.tempDirectory) return this.tempDirectory;
        var defaultPath = getDefaultTempPath();
        try {
            if (!fs.existsSync(defaultPath)) {
                fs.mkdirSync(defaultPath, { recursive: true });
            }
            this.tempDirectory = defaultPath;
            return defaultPath;
        } catch (e) {
            this.log('Error creating temp directory: ' + e.message, 'error');
            return null;
        }
    };

    MCPPremiereBridge.prototype.getDiagnosticReportPath = function() {
        var tempDir = this.getTempDirectory();
        if (!tempDir) return null;
        return path.join(tempDir, 'premiere-mcp-diagnostics-latest.json');
    };

    MCPPremiereBridge.prototype.writeDiagnosticReport = function(report) {
        try {
            var reportPath = this.getDiagnosticReportPath();
            if (!reportPath) return null;
            fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
            return reportPath;
        } catch (e) {
            this.log('Failed to write diagnostics report: ' + e.message, 'error');
            return null;
        }
    };

    // ─── File-based Command Processing ──────────────────────────────

    MCPPremiereBridge.prototype.watchDirectory = function(dirPath) {
        try {
            var files = fs.readdirSync(dirPath);
            for (var i = 0; i < files.length; i++) {
                var file = files[i];
                if (file.indexOf('command-') === 0 && file.indexOf('.json') === file.length - 5) {
                    this.processCommandFile(path.join(dirPath, file));
                    return;
                }
            }
        } catch (e) {
            this.log('Error watching directory: ' + e.message, 'error');
        }
    };

    MCPPremiereBridge.prototype.processCommandFile = function(filePath) {
        var self = this;
        try {
            var fileContent = fs.readFileSync(filePath, 'utf8');
            var command = JSON.parse(fileContent);
            this.log('Processing command: ' + command.id, 'info');
            this.addToQueue(command);
            this.isProcessing = true;
            this.executeCommand(command, function(result) {
                try {
                    var responseFile = filePath.replace('command-', 'response-');
                    fs.writeFileSync(responseFile, JSON.stringify(result, null, 2));
                    fs.unlinkSync(filePath);
                    self.log('Command completed: ' + command.id, 'info');
                    self.updateCommandStatus(command.id, 'completed');
                } catch (e) {
                    var errFile = filePath.replace('command-', 'response-');
                    fs.writeFileSync(errFile, JSON.stringify({ error: e.message, timestamp: new Date().toISOString() }, null, 2));
                }
                self.isProcessing = false;
            });
        } catch (e) {
            this.log('Error processing command file: ' + e.message, 'error');
            try {
                var responseFile = filePath.replace('command-', 'response-');
                fs.writeFileSync(responseFile, JSON.stringify({ error: e.message, timestamp: new Date().toISOString() }, null, 2));
                fs.unlinkSync(filePath);
            } catch (e2) {}
            this.isProcessing = false;
        }
    };

    // ─── Command Execution (shared by file and WebSocket) ───────────

    MCPPremiereBridge.prototype.executeCommand = function(command, done) {
        var self = this;
        this.updateCommandStatus(command.id, 'executing');
        if (!this.validateScript(command.script)) {
            done({ success: false, error: 'Script validation failed' });
            return;
        }
        this.executeExtendScript(command.script, function(err, result) {
            if (err) {
                done({ success: false, error: err.message });
                return;
            }
            done({ success: true, result: result, timestamp: new Date().toISOString() });
        });
    };

    MCPPremiereBridge.prototype.executeExtendScript = function(script, callback) {
        var self = this;
        try {
            if (!this.csInterface) {
                callback(new Error('CSInterface not initialized'));
                return;
            }

            var hostEnv = this.normalizeHostEnvironment(this.csInterface.getHostEnvironment());
            if (!hostEnv) {
                callback(new Error('Could not get host environment. Is Premiere Pro running?'));
                return;
            }

            var fullScript = EXTENDSCRIPT_COMPAT_HELPERS + '\n' + script;
            this.csInterface.evalScript(fullScript, function(result) {
                self.log('EvalScript result: ' + result, 'info');

                if (result === 'EvalScript error.' || result === 'EvalScript error') {
                    callback(new Error(
                        'ExtendScript execution failed via CEP evalScript(). ' +
                        'This is usually a host-side scripting failure or CEP compatibility issue, not a JSON parsing problem.'
                    ));
                    return;
                }

                if (typeof result === 'string' && result.indexOf('Error') === 0) {
                    callback(new Error(result));
                    return;
                }

                try {
                    var parsed = JSON.parse(result);
                    callback(null, parsed);
                } catch (e) {
                    callback(null, result);
                }
            });
        } catch (e) {
            callback(e);
        }
    };

    MCPPremiereBridge.prototype.validateScript = function(script) {
        if (!script || typeof script !== 'string') return false;
        var dangerous = [
            /eval\s*\(/i,
            /\bnew\s+Function\s*\(/i,
            /\brequire\s*\(/i,
            /\b__dirname\b/i,
            /\b__filename\b/i,
            /\bprocess\./i,
            /\bchild_process\b/i
        ];
        for (var i = 0; i < dangerous.length; i++) {
            if (dangerous[i].test(script)) return false;
        }
        return script.length <= 500000;
    };

    // ─── File Polling ───────────────────────────────────────────────

    MCPPremiereBridge.prototype.startCommandPolling = function() {
        var self = this;
        setInterval(function() {
            if (!self.isProcessing && self.isConnected) {
                var tempPath = self.getTempDirectory();
                if (tempPath) self.watchDirectory(tempPath);
            }
        }, 250);
    };

    // ─── WebSocket Server ───────────────────────────────────────────

    MCPPremiereBridge.prototype.startWebSocketServer = function() {
        var self = this;

        if (!WebSocketServer) {
            this.log('WebSocket module not available. Cannot start WS server.', 'error');
            return;
        }

        if (this.wss) {
            this.log('WebSocket server already running on port ' + this.wsPort, 'info');
            return;
        }

        var portEl = document.getElementById('wsPort');
        this.wsPort = portEl ? parseInt(portEl.value, 10) || 9876 : 9876;

        try {
            this.wss = new WebSocketServer({ port: this.wsPort });
        } catch (e) {
            this.log('Failed to start WebSocket server: ' + e.message, 'error');
            return;
        }

        this.wss.on('listening', function() {
            self.log('WebSocket server listening on port ' + self.wsPort, 'info');
            self.log('Remote MCP clients can connect to ws://<this-machine-ip>:' + self.wsPort, 'info');
            self.updateWebSocketStatus(true);
        });

        this.wss.on('connection', function(ws) {
            var clientAddr = ws._socket ? ws._socket.remoteAddress : 'unknown';
            self.log('WebSocket client connected: ' + clientAddr, 'info');
            self.wsClients.push(ws);
            self.updateWebSocketClientCount();

            ws.on('message', function(data) {
                try {
                    var command = JSON.parse(data.toString());
                    self.log('WS command: ' + command.id, 'info');
                    self.addToQueue(command);

                    self.executeCommand(command, function(result) {
                        try {
                            ws.send(JSON.stringify({
                                id: command.id,
                                result: result,
                                timestamp: new Date().toISOString()
                            }));
                            self.updateCommandStatus(command.id, 'completed');
                        } catch (sendErr) {
                            self.log('WS send error: ' + sendErr.message, 'error');
                        }
                    });
                } catch (parseErr) {
                    self.log('WS message parse error: ' + parseErr.message, 'error');
                    try {
                        ws.send(JSON.stringify({ id: 'unknown', error: parseErr.message }));
                    } catch (e) {}
                }
            });

            ws.on('close', function() {
                self.log('WebSocket client disconnected: ' + clientAddr, 'info');
                self.wsClients = self.wsClients.filter(function(c) { return c !== ws; });
                self.updateWebSocketClientCount();
            });

            ws.on('error', function(err) {
                self.log('WebSocket client error: ' + err.message, 'error');
            });
        });

        this.wss.on('error', function(err) {
            self.log('WebSocket server error: ' + err.message, 'error');
            self.wss = null;
            self.updateWebSocketStatus(false);
        });
    };

    MCPPremiereBridge.prototype.stopWebSocketServer = function() {
        if (this.wss) {
            this.wsClients.forEach(function(ws) {
                try { ws.close(); } catch (e) {}
            });
            this.wsClients = [];
            this.wss.close();
            this.wss = null;
            this.log('WebSocket server stopped', 'info');
            this.updateWebSocketStatus(false);
            this.updateWebSocketClientCount();
        }
    };

    // ─── Bridge Start / Stop ────────────────────────────────────────

    MCPPremiereBridge.prototype.startBridge = function() {
        this.log('Starting MCP Bridge...', 'info');
        this.isConnected = true;
        this.updateUI();
        var tempPath = this.getTempDirectory();
        this.log('Watching: ' + tempPath + ' (must match PREMIERE_TEMP_DIR)', 'info');
        this.updateServerStatus(true);

        // Start file polling
        this.startCommandPolling();

        // Auto-start WebSocket if enabled
        var wsEnabled = document.getElementById('wsEnabled');
        if (wsEnabled && wsEnabled.checked) {
            this.startWebSocketServer();
        }

        this.log('Bridge ready. Connect via file bridge or WebSocket.', 'info');
    };

    MCPPremiereBridge.prototype.stopBridge = function() {
        this.log('Stopping MCP Bridge...', 'info');
        this.isConnected = false;
        this.stopWebSocketServer();
        this.updateUI();
        this.updateServerStatus(false);
    };

    // ─── Diagnostics ────────────────────────────────────────────────

    MCPPremiereBridge.prototype.runDiagnostics = function() {
        var self = this;
        var report = {
            generatedAt: new Date().toISOString(),
            panel: 'MCP Bridge (CEP)',
            tempDirectory: this.getTempDirectory(),
            transport: {
                file: true,
                websocket: {
                    available: !!WebSocketServer,
                    running: !!this.wss,
                    port: this.wsPort,
                    clients: this.wsClients.length
                }
            },
            hostEnvironment: null,
            checks: []
        };

        function addCheck(name, success, details) {
            report.checks.push({ name: name, success: success, details: details });
        }

        function finalize() {
            var reportPath = self.writeDiagnosticReport(report);
            if (reportPath) {
                self.log('Diagnostics report saved to ' + reportPath, 'info');
            }
            self.log('Diagnostics summary: ' + JSON.stringify(report), 'info');
        }

        this.log('Running CEP diagnostics...', 'info');

        try {
            var hostEnvironment = this.normalizeHostEnvironment(this.csInterface.getHostEnvironment());
            report.hostEnvironment = hostEnvironment;
            addCheck('host_environment', !!hostEnvironment, hostEnvironment || 'No host environment returned');
        } catch (e) {
            addCheck('host_environment', false, e.message);
            finalize();
            return;
        }

        var checks = [
            {
                name: 'eval_string',
                script: '(function(){ return "cep-ok"; })();'
            },
            {
                name: 'app_version_raw',
                script: '(function(){ try { return app.version; } catch (e) { return "ERROR: " + String(e); } })();'
            },
            {
                name: 'eval_json_roundtrip',
                script: '(function(){ return JSON.stringify({ ok: true, transport: "cep" }); })();'
            },
            {
                name: 'app_version',
                script: '(function(){ try { return JSON.stringify({ appVersion: app.version, appName: app.name }); } catch (e) { return JSON.stringify({ error: String(e) }); } })();'
            },
            {
                name: 'project_access',
                script: '(function(){ try { return JSON.stringify({ projectName: (app.project && app.project.name) ? app.project.name : "No project open" }); } catch (e) { return JSON.stringify({ error: String(e) }); } })();'
            }
        ];

        function runCheck(index) {
            if (index >= checks.length) {
                finalize();
                return;
            }

            var check = checks[index];
            self.executeExtendScript(check.script, function(err, result) {
                if (err) {
                    addCheck(check.name, false, err.message);
                } else {
                    addCheck(check.name, true, result);
                }
                runCheck(index + 1);
            });
        }

        runCheck(0);
    };

    MCPPremiereBridge.prototype.testPremiereConnection = function() {
        var self = this;
        var script = '(function() {\
            try {\
                var d = new Date();\
                var timestamp = d.getFullYear() + "-" + \
                    String(d.getMonth() + 1).replace(/^(\\d)$/, "0$1") + "-" + \
                    String(d.getDate()).replace(/^(\\d)$/, "0$1") + "T" + \
                    String(d.getHours()).replace(/^(\\d)$/, "0$1") + ":" + \
                    String(d.getMinutes()).replace(/^(\\d)$/, "0$1") + ":" + \
                    String(d.getSeconds()).replace(/^(\\d)$/, "0$1");\
                var info = {\
                    appVersion: app.version,\
                    projectName: "No project open",\
                    timestamp: timestamp\
                };\
                try {\
                    if (app.project && app.project.name) {\
                        info.projectName = app.project.name;\
                    }\
                } catch(e) {}\
                return JSON.stringify(info);\
            } catch(e) {\
                return JSON.stringify({ error: String(e) });\
            }\
        })();';
        this.executeExtendScript(script, function(err, result) {
            if (err) {
                self.log('Premiere Pro connection failed: ' + err.message, 'error');
                self.updateServerStatus(false);
            } else {
                self.log('Premiere Pro connection OK: ' + JSON.stringify(result), 'info');
                self.updateServerStatus(true);
            }
        });
    };

    // ─── UI Updates ─────────────────────────────────────────────────

    MCPPremiereBridge.prototype.addToQueue = function(command) {
        this.commandQueue.push({ id: command.id, status: 'pending', script: (command.script || '').substring(0, 50) + '...' });
        this.updateCommandQueueUI();
    };

    MCPPremiereBridge.prototype.updateCommandStatus = function(commandId, status) {
        for (var i = 0; i < this.commandQueue.length; i++) {
            if (this.commandQueue[i].id === commandId) {
                this.commandQueue[i].status = status;
                break;
            }
        }
        this.updateCommandQueueUI();
    };

    MCPPremiereBridge.prototype.updateCommandQueueUI = function() {
        var el = document.getElementById('commandQueue');
        if (!el) return;
        if (this.commandQueue.length === 0) {
            el.innerHTML = '<div class="command-item"><span class="command-label">No commands in queue</span></div>';
            return;
        }
        var html = this.commandQueue.slice(-5).map(function(cmd) {
            return '<div class="command-item"><span class="command-label">' + cmd.script + '</span><span class="command-status ' + cmd.status + '">' + cmd.status + '</span></div>';
        }).join('');
        el.innerHTML = html;
    };

    MCPPremiereBridge.prototype.updateUI = function() {
        var connectionStatus = document.getElementById('connectionStatus');
        var connectionText = document.getElementById('connectionText');
        if (connectionStatus && connectionText) {
            if (this.isConnected) {
                connectionStatus.className = 'status-dot connected';
                connectionText.textContent = 'Connected';
            } else {
                connectionStatus.className = 'status-dot disconnected';
                connectionText.textContent = 'Disconnected';
            }
        }
        var startBtn = document.getElementById('startButton');
        var stopBtn = document.getElementById('stopButton');
        if (startBtn) startBtn.disabled = this.isConnected;
        if (stopBtn) stopBtn.disabled = !this.isConnected;
        var tempEl = document.getElementById('tempDirectory');
        if (tempEl && !tempEl.value && this.getTempDirectory()) tempEl.value = this.getTempDirectory();
    };

    MCPPremiereBridge.prototype.updateServerStatus = function(isRunning) {
        var serverStatus = document.getElementById('serverStatus');
        var serverText = document.getElementById('serverText');
        if (serverStatus && serverText) {
            if (isRunning) {
                serverStatus.className = 'status-dot connected';
                serverText.textContent = 'Premiere Pro: Ready';
            } else {
                serverStatus.className = 'status-dot disconnected';
                serverText.textContent = 'Premiere Pro: Start Bridge to enable';
            }
        }
    };

    MCPPremiereBridge.prototype.updateWebSocketStatus = function(isRunning) {
        var wsStatus = document.getElementById('wsStatus');
        var wsText = document.getElementById('wsText');
        if (wsStatus && wsText) {
            if (isRunning) {
                wsStatus.className = 'status-dot connected';
                wsText.textContent = 'WS: Port ' + this.wsPort;
            } else {
                wsStatus.className = 'status-dot disconnected';
                wsText.textContent = 'WS: Off';
            }
        }
    };

    MCPPremiereBridge.prototype.updateWebSocketClientCount = function() {
        var el = document.getElementById('wsClientCount');
        if (el) {
            el.textContent = this.wsClients.length + ' client(s)';
        }
    };

    MCPPremiereBridge.prototype.loadConfig = function() {
        try {
            var defaultPath = getDefaultTempPath();
            if (fs.existsSync(defaultPath)) {
                var configPath = path.join(defaultPath, 'config.json');
                if (fs.existsSync(configPath)) {
                    var config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
                    if (config.tempDirectory) this.tempDirectory = config.tempDirectory;
                    if (config.wsPort) this.wsPort = config.wsPort;
                    if (config.wsEnabled !== undefined) {
                        var wsEnabledEl = document.getElementById('wsEnabled');
                        if (wsEnabledEl) wsEnabledEl.checked = config.wsEnabled;
                    }
                }
            }
            var tempEl = document.getElementById('tempDirectory');
            if (tempEl && this.tempDirectory) tempEl.value = this.tempDirectory;
            var portEl = document.getElementById('wsPort');
            if (portEl) portEl.value = this.wsPort;
        } catch (e) {}
    };

    MCPPremiereBridge.prototype.saveConfig = function() {
        try {
            var tempEl = document.getElementById('tempDirectory');
            var tempDir = tempEl ? tempEl.value.trim() : '';
            if (tempDir) this.tempDirectory = tempDir;

            var portEl = document.getElementById('wsPort');
            if (portEl) this.wsPort = parseInt(portEl.value, 10) || 9876;

            var wsEnabledEl = document.getElementById('wsEnabled');
            var wsEnabled = wsEnabledEl ? wsEnabledEl.checked : false;

            var configPath = path.join(this.getTempDirectory(), 'config.json');
            fs.writeFileSync(configPath, JSON.stringify({
                tempDirectory: this.tempDirectory,
                wsPort: this.wsPort,
                wsEnabled: wsEnabled
            }, null, 2));
            this.log('Configuration saved', 'info');
        } catch (e) {
            this.log('Error saving config: ' + e.message, 'error');
        }
    };

    MCPPremiereBridge.prototype.log = function(message, level) {
        level = level || 'info';
        var logContainer = document.getElementById('logContainer');
        if (logContainer) {
            var el = document.createElement('div');
            el.className = 'log-entry ' + level;
            el.textContent = '[' + new Date().toISOString() + '] ' + message;
            logContainer.appendChild(el);
            logContainer.scrollTop = logContainer.scrollHeight;
        }
        console.log(message);
    };

    MCPPremiereBridge.prototype.clearLog = function() {
        var logContainer = document.getElementById('logContainer');
        if (logContainer) logContainer.innerHTML = '<div class="log-entry info">Log cleared</div>';
    };

    window.MCPPremiereBridge = MCPPremiereBridge;
    window.bridge = null;
    window.startBridge = function() { if (window.bridge) window.bridge.startBridge(); };
    window.stopBridge = function() { if (window.bridge) window.bridge.stopBridge(); };
    window.runDiagnostics = function() { if (window.bridge) window.bridge.runDiagnostics(); };
    window.saveConfig = function() { if (window.bridge) window.bridge.saveConfig(); };
    window.clearLog = function() { if (window.bridge) window.bridge.clearLog(); };
    document.addEventListener('DOMContentLoaded', function() {
        window.bridge = new MCPPremiereBridge();
    });
})();
