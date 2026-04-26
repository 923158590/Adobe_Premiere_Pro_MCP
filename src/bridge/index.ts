/**
 * Bridge module for communicating with Adobe Premiere Pro
 *
 * This module handles the communication between the MCP server and Adobe Premiere Pro
 * using WebSocket or file-based communication.
 *
 * Transport is selected via environment variables:
 *   PREMIERE_BRIDGE_URL=ws://host:port  → WebSocket mode (remote-capable)
 *   (no PREMIERE_BRIDGE_URL)             → File mode (original local behaviour)
 */

import { Logger } from '../utils/logger.js';
import { ChildProcess } from 'child_process';
import { promises as fs } from 'fs';
import { join } from 'path';
import { v4 as uuidv4 } from 'uuid';
import WebSocket from 'ws';
import { createSecureTempDir, validateFilePath } from '../utils/security.js';
import type { PremiereProTransport } from './types.js';

const EXTENDSCRIPT_HELPERS = `
function __mcpEscapeString(value) {
  return String(value)
    .replace(/\\\\/g, '\\\\\\\\')
    .replace(/"/g, '\\\\"')
    .replace(/\\r/g, '\\\\r')
    .replace(/\\n/g, '\\\\n')
    .replace(/\\t/g, '\\\\t');
}
function __mcpStringify(value) {
  if (value === null) return 'null';
  var valueType = typeof value;
  if (valueType === 'string') return '"' + __mcpEscapeString(value) + '"';
  if (valueType === 'number') return isFinite(value) ? String(value) : 'null';
  if (valueType === 'boolean') return value ? 'true' : 'false';
  if (value instanceof Array) {
    var arrayParts = [];
    for (var i = 0; i < value.length; i++) {
      arrayParts.push(__mcpStringify(value[i]));
    }
    return '[' + arrayParts.join(',') + ']';
  }
  if (valueType === 'object') {
    var objectParts = [];
    for (var key in value) {
      if (value.hasOwnProperty && !value.hasOwnProperty(key)) continue;
      if (typeof value[key] === 'undefined' || typeof value[key] === 'function') continue;
      objectParts.push(__mcpStringify(String(key)) + ':' + __mcpStringify(value[key]));
    }
    return '{' + objectParts.join(',') + '}';
  }
  return 'null';
}
if (typeof JSON === 'undefined') { JSON = {}; }
if (typeof JSON.stringify !== 'function') { JSON.stringify = __mcpStringify; }
function __findSequence(id) {
  for (var i = 0; i < app.project.sequences.numSequences; i++) {
    if (app.project.sequences[i].sequenceID === id) return app.project.sequences[i];
  }
  return null;
}
function __findClip(nodeId) {
  var seq = app.project.activeSequence;
  if (!seq) return null;
  for (var t = 0; t < seq.videoTracks.numTracks; t++) {
    var track = seq.videoTracks[t];
    for (var c = 0; c < track.clips.numItems; c++) {
      if (track.clips[c].nodeId === nodeId)
        return { clip: track.clips[c], track: track, trackIndex: t, clipIndex: c, trackType: 'video' };
    }
  }
  for (var t = 0; t < seq.audioTracks.numTracks; t++) {
    var track = seq.audioTracks[t];
    for (var c = 0; c < track.clips.numItems; c++) {
      if (track.clips[c].nodeId === nodeId)
        return { clip: track.clips[c], track: track, trackIndex: t, clipIndex: c, trackType: 'audio' };
    }
  }
  return null;
}
function __findProjectItem(nodeId) {
  function walk(item) {
    if (item.nodeId === nodeId) return item;
    if (item.children) {
      for (var i = 0; i < item.children.numItems; i++) {
        var found = walk(item.children[i]);
        if (found) return found;
      }
    }
    return null;
  }
  return walk(app.project.rootItem);
}
function __ticksToSeconds(ticks) {
  return parseInt(ticks, 10) / 254016000000;
}
function __secondsToTicks(seconds) {
  return String(Math.round(seconds * 254016000000));
}
`;

export interface PremiereProProject {
  id: string;
  name: string;
  path: string;
  isOpen: boolean;
  sequences: PremiereProSequence[];
  projectItems: PremiereProProjectItem[];
}

export interface PremiereProSequence {
  id: string;
  name: string;
  duration: number;
  frameRate: number;
  videoTracks: PremiereProTrack[];
  audioTracks: PremiereProTrack[];
}

export interface PremiereProTrack {
  id: string;
  name: string;
  type: 'video' | 'audio';
  clips: PremiereProClip[];
}

export interface PremiereProClip {
  id: string;
  name: string;
  inPoint: number;
  outPoint: number;
  duration: number;
  mediaPath?: string;
}

export interface PremiereProProjectItem {
  id: string;
  name: string;
  type: 'footage' | 'sequence' | 'bin';
  mediaPath?: string;
  duration?: number;
  frameRate?: number;
}

export interface PremiereProEffect {
  id: string;
  name: string;
  category: string;
  parameters: Record<string, any>;
}

export class PremiereProBridge implements PremiereProTransport {
  private logger: Logger;
  private transportMode: 'file' | 'websocket';
  private tempDir: string;
  private readonly usesExternalTempDir: boolean;
  private uxpProcess?: ChildProcess;
  private isInitialized = false;
  private sessionId: string;

  // WebSocket transport state
  private ws: WebSocket | null = null;
  private readonly bridgeUrl: string | null = null;
  private wsReconnectTimer: ReturnType<typeof setTimeout> | null = null;

  constructor() {
    this.logger = new Logger('PremiereProBridge');
    this.sessionId = uuidv4();

    const bridgeUrl = process.env.PREMIERE_BRIDGE_URL;
    if (bridgeUrl) {
      this.transportMode = 'websocket';
      this.bridgeUrl = bridgeUrl;
      const envDir = process.env.PREMIERE_TEMP_DIR;
      this.usesExternalTempDir = Boolean(envDir);
      this.tempDir = envDir ? envDir.replace(/\/$/, '') : '/tmp/premiere-mcp-bridge';
    } else {
      this.transportMode = 'file';
      this.bridgeUrl = null;
      const envDir = process.env.PREMIERE_TEMP_DIR;
      this.usesExternalTempDir = Boolean(envDir);
      this.tempDir = envDir ? envDir.replace(/\/$/, '') : createSecureTempDir(this.sessionId);
    }
  }

  async initialize(): Promise<void> {
    try {
      if (this.transportMode === 'websocket') {
        await this.connectWebSocket();
      } else {
        await this.setupTempDirectory();
      }
      this.isInitialized = true;
      this.logger.info(`Premiere Pro bridge initialized (${this.transportMode} mode)`);
    } catch (error) {
      this.logger.error('Failed to initialize bridge:', error);
      throw error;
    }
  }

  // ─── WebSocket Transport ──────────────────────────────────────────

  private connectWebSocket(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.bridgeUrl) {
        reject(new Error('PREMIERE_BRIDGE_URL is not set'));
        return;
      }
      this.logger.info(`Connecting to CEP bridge at ${this.bridgeUrl}...`);
      this.ws = new WebSocket(this.bridgeUrl);

      const timeout = setTimeout(() => {
        reject(new Error(`WebSocket connection timeout to ${this.bridgeUrl}`));
      }, 10000);

      this.ws.on('open', () => {
        clearTimeout(timeout);
        this.logger.info('WebSocket connected to CEP bridge');
        resolve();
      });

      this.ws.on('error', (err) => {
        clearTimeout(timeout);
        this.logger.error(`WebSocket error: ${err.message}`);
        if (!this.isInitialized) reject(err);
      });

      this.ws.on('close', () => {
        this.logger.warn('WebSocket disconnected');
        this.scheduleReconnect();
      });
    });
  }

  private scheduleReconnect(): void {
    if (this.wsReconnectTimer) return;
    this.wsReconnectTimer = setTimeout(() => {
      this.wsReconnectTimer = null;
      if (!this.bridgeUrl) return;
      this.logger.info('Attempting WebSocket reconnect...');
      this.ws = new WebSocket(this.bridgeUrl);

      this.ws.on('open', () => {
        this.logger.info('WebSocket reconnected');
      });
      this.ws.on('error', (err) => {
        this.logger.error(`WebSocket reconnect error: ${err.message}`);
      });
      this.ws.on('close', () => {
        this.scheduleReconnect();
      });
    }, 3000);
  }

  private executeScriptViaWebSocket(script: string): Promise<any> {
    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        reject(new Error('WebSocket not connected. Ensure CEP bridge panel is open and started.'));
        return;
      }

      const commandId = uuidv4();
      const fullScript = this.buildExecutableScript(script);

      const timer = setTimeout(() => {
        this.ws!.off('message', handler);
        reject(new Error('WebSocket response timeout (60s)'));
      }, 60000);

      const handler = (data: WebSocket.Data) => {
        try {
          const msg = JSON.parse(data.toString());
          if (msg.id === commandId) {
            clearTimeout(timer);
            this.ws!.off('message', handler);
            if (msg.error) {
              reject(new Error(msg.error));
            } else {
              resolve(msg.result !== undefined ? msg.result : msg);
            }
          }
        } catch {
          // ignore non-matching or malformed messages
        }
      };

      this.ws!.on('message', handler);
      this.ws!.send(JSON.stringify({
        id: commandId,
        script: fullScript,
        timestamp: new Date().toISOString()
      }));
    });
  }

  // ─── File Transport (original) ────────────────────────────────────

  private async setupTempDirectory(): Promise<void> {
    try {
      await fs.mkdir(this.tempDir, { recursive: true, mode: 0o700 });
      this.logger.debug(`Temp directory ready: ${this.tempDir}`);
    } catch (error) {
      this.logger.error('Failed to create temp directory:', error);
      throw error;
    }
  }

  // ─── Script Helpers ───────────────────────────────────────────────

  private isSelfInvokingScript(script: string): boolean {
    const trimmed = script.trim();
    return /^\(function\s*\(\)\s*\{[\s\S]*\}\)\s*\(\)\s*;?$/.test(trimmed);
  }

  private buildExecutableScript(script: string): string {
    if (this.isSelfInvokingScript(script)) {
      return EXTENDSCRIPT_HELPERS + script.trim();
    }
    return EXTENDSCRIPT_HELPERS + '(function(){\n' + script + '\n})();';
  }

  // ─── Unified executeScript ────────────────────────────────────────

  async executeScript(script: string): Promise<any> {
    if (!this.isInitialized) {
      throw new Error('Bridge not initialized. Call initialize() first.');
    }

    if (this.transportMode === 'websocket') {
      return this.executeScriptViaWebSocket(script);
    }

    // File-based transport
    const commandId = uuidv4();
    const commandFile = join(this.tempDir, `command-${commandId}.json`);
    const responseFile = join(this.tempDir, `response-${commandId}.json`);

    try {
      const fullScript = this.buildExecutableScript(script);

      await fs.writeFile(commandFile, JSON.stringify({
        id: commandId,
        script: fullScript,
        timestamp: new Date().toISOString()
      }));

      const response = await this.waitForResponse(responseFile);

      await fs.unlink(commandFile).catch(() => {});
      await fs.unlink(responseFile).catch(() => {});

      return response;
    } catch (error) {
      this.logger.error(`Script execution failed: ${error}`);
      throw error;
    }
  }

  private async waitForResponse(responseFile: string, timeout = 60000): Promise<any> {
    const startTime = Date.now();

    while (Date.now() - startTime < timeout) {
      try {
        const response = await fs.readFile(responseFile, 'utf8');
        const parsed = JSON.parse(response);
        if (parsed.result !== undefined) return parsed.result;
        return parsed;
      } catch {
        await new Promise(resolve => setTimeout(resolve, 150));
      }
    }

    throw new Error(
      'Bridge response timeout. Ensure Premiere Pro is open, MCP Bridge (CEP or UXP) panel is open, ' +
      'Temp Directory is set to ' + this.tempDir + ', and Start Bridge is clicked.'
    );
  }

  // ─── Project Management ───────────────────────────────────────────

  async createProject(name: string, location: string): Promise<PremiereProProject> {
    const script = `
      app.newProject("${name}", "${location}");
      var project = app.project;
      return JSON.stringify({
        id: project.documentID,
        name: project.name,
        path: project.path,
        isOpen: true,
        sequences: [],
        projectItems: []
      });
    `;
    return await this.executeScript(script);
  }

  async openProject(path: string): Promise<PremiereProProject> {
    const script = `
      app.openDocument("${path}");
      var project = app.project;
      return JSON.stringify({
        id: project.documentID,
        name: project.name,
        path: project.path,
        isOpen: true,
        sequences: [],
        projectItems: []
      });
    `;
    return await this.executeScript(script);
  }

  async saveProject(): Promise<void> {
    const script = `
      app.project.save();
      return JSON.stringify({ success: true });
    `;
    await this.executeScript(script);
  }

  async importMedia(filePath: string): Promise<PremiereProProjectItem> {
    const pathValidation = validateFilePath(filePath);
    if (!pathValidation.valid) {
      throw new Error(`Invalid file path: ${pathValidation.error}`);
    }

    const safePath = pathValidation.normalized || filePath;
    const script = `
      try {
        function __walkItems(parent, output) {
          for (var i = 0; i < parent.children.numItems; i++) {
            var child = parent.children[i];
            output.push(child);
            if (child.type === ProjectItemType.BIN) {
              __walkItems(child, output);
            }
          }
        }

        var file = new File(${JSON.stringify(safePath)});
        if (!file.exists) {
          return JSON.stringify({
            success: false,
            error: "File not found: " + ${JSON.stringify(safePath)}
          });
        }

        var existingItems = [];
        __walkItems(app.project.rootItem, existingItems);

        var importResult = app.project.importFiles([file.fsName], true, app.project.rootItem, false);
        if (!importResult) {
          return JSON.stringify({
            success: false,
            error: "Failed to import file"
          });
        }

        var afterItems = [];
        __walkItems(app.project.rootItem, afterItems);

        var importedItem = null;
        for (var j = 0; j < afterItems.length; j++) {
          var candidate = afterItems[j];
          var alreadyPresent = false;
          for (var k = 0; k < existingItems.length; k++) {
            if (existingItems[k].nodeId === candidate.nodeId) {
              alreadyPresent = true;
              break;
            }
          }
          if (alreadyPresent) {
            continue;
          }
          try {
            if (candidate.getMediaPath && candidate.getMediaPath() === file.fsName) {
              importedItem = candidate;
              break;
            }
          } catch (e) {}
          if (!importedItem && candidate.name === file.name) {
            importedItem = candidate;
          }
        }

        if (!importedItem) {
          return JSON.stringify({
            success: false,
            error: "Import completed but imported item could not be located"
          });
        }

        return JSON.stringify({
          success: true,
          id: importedItem.nodeId,
          name: importedItem.name,
          type: importedItem.type.toString(),
          mediaPath: importedItem.getMediaPath ? importedItem.getMediaPath() : file.fsName
        });
      } catch (e) {
        return JSON.stringify({
          success: false,
          error: e.toString()
        });
      }
    `;

    return await this.executeScript(script);
  }

  async createSequence(name: string, presetPath?: string): Promise<PremiereProSequence> {
    const script = `
      var sequence = app.project.createNewSequence("${name}", "${presetPath || ''}");
      return JSON.stringify({
        id: sequence.sequenceID,
        name: sequence.name,
        duration: sequence.end - sequence.zeroPoint,
        frameRate: sequence.framerate,
        videoTracks: [],
        audioTracks: []
      });
    `;
    return await this.executeScript(script);
  }

  async addToTimeline(sequenceId: string, projectItemId: string, trackIndex: number, time: number): Promise<PremiereProClip> {
    const script = `
      try {
        var sequence = __findSequence("${sequenceId}");
        if (!sequence) {
          return JSON.stringify({ success: false, error: "Sequence not found" });
        }

        var projectItem = __findProjectItem("${projectItemId}");
        if (!projectItem) {
          return JSON.stringify({ success: false, error: "Project item not found" });
        }

        var track = sequence.videoTracks[${trackIndex}];
        if (!track) {
          return JSON.stringify({ success: false, error: "Video track not found" });
        }

        track.overwriteClip(projectItem, ${time});

        var placedClip = null;
        for (var i = 0; i < track.clips.numItems; i++) {
          var candidate = track.clips[i];
          if (candidate && candidate.projectItem && candidate.projectItem.nodeId === projectItem.nodeId && Math.abs(candidate.start.seconds - ${time}) < 0.1) {
            placedClip = candidate;
            break;
          }
        }

        if (!placedClip && track.clips.numItems > 0) {
          placedClip = track.clips[track.clips.numItems - 1];
        }

        if (!placedClip) {
          return JSON.stringify({ success: false, error: "Clip placement did not produce a track item" });
        }

        return JSON.stringify({
          success: true,
          id: placedClip.nodeId,
          name: placedClip.name,
          inPoint: placedClip.start.seconds,
          outPoint: placedClip.end.seconds,
          duration: placedClip.duration.seconds,
          mediaPath: placedClip.projectItem && placedClip.projectItem.getMediaPath ? placedClip.projectItem.getMediaPath() : ""
        });
      } catch (e) {
        return JSON.stringify({
          success: false,
          error: e.toString()
        });
      }
    `;
    return await this.executeScript(script);
  }

  async renderSequence(sequenceId: string, outputPath: string, presetPath: string): Promise<void> {
    const script = `
      var sequence = app.project.getSequenceByID("${sequenceId}");
      var encoder = app.encoder;
      encoder.encodeSequence(sequence, "${outputPath}", "${presetPath}",
        encoder.ENCODE_ENTIRE, false);
      return JSON.stringify({ success: true });
    `;
    await this.executeScript(script);
  }

  async listProjectItems(): Promise<PremiereProProjectItem[]> {
    const script = `
      try {
        if (!app.project || !app.project.rootItem) {
          throw new Error('No open project');
        }
        function walk(item) {
          var results = [];
          if (item.type === ProjectItemType.BIN) {
            for (var i = 0; i < item.children.numItems; i++) {
              results = results.concat(walk(item.children[i]));
            }
          } else {
            results.push({
              id: item.nodeId || item.treePath || item.name,
              name: item.name,
              type: item.type === ProjectItemType.BIN ? 'bin' : (item.type === ProjectItemType.SEQUENCE ? 'sequence' : 'footage'),
              mediaPath: item.getMediaPath ? item.getMediaPath() : undefined,
              duration: item.getOutPoint ? (item.getOutPoint() - item.getInPoint()) : undefined,
              frameRate: item.getVideoFrameRate ? item.getVideoFrameRate() : undefined
            });
          }
          return results;
        }
        var items = walk(app.project.rootItem);
        return JSON.stringify({ ok: true, items: items });
      } catch (e) {
        return JSON.stringify({ ok: false, error: String(e) });
      }
    `;
    const result = await this.executeScript(script);
    if (result.ok) return result.items;
    throw new Error(result.error || 'Unknown error listing project items');
  }

  async cleanup(): Promise<void> {
    // Close WebSocket
    if (this.wsReconnectTimer) {
      clearTimeout(this.wsReconnectTimer);
      this.wsReconnectTimer = null;
    }
    if (this.ws) {
      this.ws.removeAllListeners();
      this.ws.close();
      this.ws = null;
    }

    if (this.uxpProcess) {
      this.uxpProcess.kill();
    }

    // Only remove temp dirs created by this server
    try {
      if (!this.usesExternalTempDir) {
        await fs.rm(this.tempDir, { recursive: true });
      }
    } catch (error) {
      this.logger.warn('Failed to clean up temp directory:', error);
    }

    this.logger.info('Premiere Pro bridge cleaned up');
  }
}
