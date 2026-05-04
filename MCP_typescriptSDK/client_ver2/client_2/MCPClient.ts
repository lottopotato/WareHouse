import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import type {
  CallToolRequest,
  ReadResourceRequest,
  GetPromptRequest
} from '@modelcontextprotocol/sdk/types.js';
import {
  CallToolResultSchema,
  CompatibilityCallToolResultSchema,
  ClientNotificationSchema,
  ServerNotificationSchema,
  NotificationSchema as BaseNotificationSchema,
  CancelledNotificationSchema,
  LoggingMessageNotificationSchema,
  ResourceUpdatedNotificationSchema,
  ResourceListChangedNotificationSchema,
  ToolListChangedNotificationSchema,
  PromptListChangedNotificationSchema,
} from '@modelcontextprotocol/sdk/types.js';
import type { RequestOptions } from '@modelcontextprotocol/sdk/shared/protocol.js';
import axios from 'axios';
import { z } from 'zod';

import type { MCPClientConfig } from './Interface';

const NotificationSchema = ClientNotificationSchema.or(
  ServerNotificationSchema).or(
    BaseNotificationSchema);
type Notification = z.infer<typeof NotificationSchema>;

const CONNECTION_ESTABLISH_TIMEOUT_MS = 30_000; // 30 seconds to establish a connection
const BASE_RETRY_DELAY_MS = 500; // 0.5 seconds initial retry delay
const MAX_RETRY_DELAY_MS = 8000; // Max 8 seconds delay between retries

function timeoutPromise<T>(ms: number, message: string = 'Operation timed out'): Promise<T> {
  return new Promise((_, reject) => {
    const id = setTimeout(() => {
      clearTimeout(id);
      reject(new Error(message));
    }, ms);
  });
};

class MCPClient {
  private readonly id: string;
  private readonly client: Client;
  private readonly connectionUrl: string;
  private readonly authenticationToken: string | undefined;
  private transport: StreamableHTTPClientTransport | undefined;
  private sessionId: string | undefined;
  private onNotification?: (notification: Notification) => void;
  private isCallingWithTimeout: boolean = false;
  private isConnecting: boolean = false;

  public isConnected: boolean = false;
  public jobMaxRetries: number;
  public verbose: boolean;
  public connectionTimeout: number = 30;
  public lastActivityTime: Date = new Date();
  public inactivityTimer: any;


  /**
   * MCPClient constructor
   * @param config - The configuration for the MCP client
   * @param authenticationToken - The authentication token for the client
   * @param verbose - Whether to enable verbose logging
   * @param onNotification - Callback for handling notifications
   * @param connectionTimeout - The connection timeout in seconds: 
   * If set to greater than 30, the client will automatically disconnect after this amount of time if there is no activity.
   * And If set to greater than 0 and less than 30, the client will perform a time check every 30 seconds, causing the connection to be disconnected after 30 seconds.
   * Otherwise, If set to less than -1, the client will not automatically disconnect.
   */

  constructor(
    config: MCPClientConfig,
    authenticationToken: string | undefined,
    verbose: boolean = false,
    onNotification?: (notification: Notification) => void,
    connectionTimeout?: number,
  ) {
    this.id = config.clientId;
    this.authenticationToken = authenticationToken;
    this.client = new Client(config.clientInfo);
    this.transport = undefined;
    this.sessionId = undefined;
    this.verbose = verbose;
    this.jobMaxRetries = 2;
    this.connectionUrl = config.connectionUrl;

    // Notification
    this.onNotification = onNotification;
    if (this.onNotification) {
      [
        CancelledNotificationSchema,
        LoggingMessageNotificationSchema,
        ResourceUpdatedNotificationSchema,
        ResourceListChangedNotificationSchema,
        ToolListChangedNotificationSchema,
        PromptListChangedNotificationSchema,
      ].forEach((schema) => {
        this.onNotification && this.client.setNotificationHandler(
          schema, this.onNotification
        )
      })
      this.client.fallbackNotificationHandler = (
        notification: Notification
      ): Promise<void> => {
        this.onNotification && this.onNotification(notification);
        return Promise.resolve();
      }
    };

    // connection timer
    this.lastActivityTime = new Date();
    if (connectionTimeout !== undefined) {
      if (connectionTimeout > 0 && connectionTimeout < 30) {
        this.connectionTimeout = 30;
      } else {
        this.connectionTimeout = connectionTimeout;
      }
    }
  };

  private startInactivityTimer() {
    if (this.isCallingWithTimeout) {
      return; // Do not start timer during timeout calls
    }

    this.clearInactivityTimer();

    if (this.connectionTimeout < 0) {
      return; // No timeout set
    };

    this.inactivityTimer = setInterval(() => {
      const now = new Date();
      const timeSinceLastActivity = now.getTime() - this.lastActivityTime.getTime();
      if (timeSinceLastActivity > this.connectionTimeout * 1000) {
        if (this.verbose) {
          console.log(`MCPClient '${this.id}' inactive for ${timeSinceLastActivity / 1000} seconds. Disconnecting...`);
        }
        this.disconnect();
      }
    }, 1000 * 30); // Check every 30 seconds
  };

  private clearInactivityTimer() {
    if (0 <= this.connectionTimeout && this.inactivityTimer) {
      try {
        clearInterval(this.inactivityTimer);
      } catch (e) {
      } finally {
        this.inactivityTimer = undefined;
      }
    }
  };

  public getInformation() {
    console.log(this.client)
    if (this.transport) console.log(this.transport);
  };

  public async connect(): Promise<void> {
    if (this.isConnected) {
      return;
    }
    if (this.isConnecting) {
      if (this.verbose) {
        console.log(`MCPClient '${this.id}' is already connecting. Please wait...`);
      }
      return;
    }
    this.isConnecting = true;
    if (this.verbose) {
      console.log(`MCPClient '${this.id}' attempting to connect via ${this.transport}...`);
    }

    try {
      this.transport = new StreamableHTTPClientTransport(
        new URL(`${this.connectionUrl}/mcp`)
      );

      await Promise.race([
        this.client.connect(this.transport),
        timeoutPromise(CONNECTION_ESTABLISH_TIMEOUT_MS, 'Connection establishment timed out')
      ]);

      if (this.transport.sessionId) {
        this.sessionId = this.transport.sessionId;
      }
      this.isConnected = true;
      if (this.verbose) {
        console.log(`MCPClient '${this.id}' connected.`);
      }
      this.lastActivityTime = new Date();
      this.startInactivityTimer();
    } catch (error) {
      console.error(`MCPClient '${this.id}' failed to connect:`, error);
      this.isConnected = false;
      this.sessionId = undefined;
      this.clearInactivityTimer();
      throw error;
    } finally {
      this.isConnecting = false;
    }
  };

  public async connectTest(): Promise<string | null | undefined> {
    const clientVerbose = this.verbose;
    this.verbose = true;
    try {
      if (!this.isConnected) {
        await this.connect()
      };
      if (this.transport && 'sessionId' in this.transport && this.transport.sessionId) {
        this.sessionId = this.transport.sessionId;
        return this.sessionId;
      };
      await this.disconnect();
      return null;
    } catch (error) {
      console.error(`MCPClient '${this.id}' connection test failed:`, error);
      return null;
    } finally {
      this.verbose = clientVerbose;
    };
  }

  public async disconnect(): Promise<any> {
    if (!this.isConnected) {
      return;
    }
    if (this.verbose) {
      console.log(`MCPClient '${this.id}' disconnecting...`);
    }
    this.clearInactivityTimer();

    try {
      // const result = await this.client.callTool({
      //   name: 'exclude_server_instance',
      //   arguments: { 
      //     token: this.authenticationToken, 
      //     sessionId: this.sessionId
      //   }
      // });
      if (this.transport) {
        await this.client.close();
      };
      this.isConnected = false;
      this.sessionId = undefined;
      this.transport = undefined;
      if (this.verbose) {
        console.log(`MCPClient '${this.id}' disconnected.`);
      }
      // if (result) {
      //   return result;
      // }
    } catch (error) {
      // await this.connect();
      console.error(`MCPClient '${this.id}' failed to disconnect:`, error);
      this.isConnected = false;
      this.sessionId = undefined;
      this.transport = undefined;
      throw error;
    } finally {
      this.isConnecting = false;
    };
    return;
  };

  public async abortAllRequests(): Promise<void> {
    if (this.verbose) {
      console.log(`MCPClient '${this.id}' aborting all requests...`);
    }
    await this.disconnect();
  };

  private async delayRetry(attempt: number): Promise<void> {
    const delay = Math.min(
      BASE_RETRY_DELAY_MS * Math.pow(2, attempt - 1),
      MAX_RETRY_DELAY_MS
    );
    if (this.verbose) {
      console.log(`Retrying in ${delay} ms...`);
    }
    await new Promise((resolve) => setTimeout(resolve, delay));
  };

  public async callTool(
    params: CallToolRequest["params"],
    resultSchema?: typeof CallToolResultSchema | typeof CompatibilityCallToolResultSchema,
    options?: RequestOptions,
    jobMaxRetries?: number
  ): Promise<any> {
    this.lastActivityTime = new Date();
    if (this.verbose) {
      console.log(`params: ${JSON.stringify(params)}`);
    };

    const effectiveMaxRetires = jobMaxRetries !== undefined
      ? Math.max(1, jobMaxRetries)
      : Math.max(1, this.jobMaxRetries);
    this.isCallingWithTimeout = options?.timeout !== undefined && options.timeout > 0;
    if (this.isCallingWithTimeout) {
      this.clearInactivityTimer();
      // Stop inactivity timer during timeout calls
    };
    let attempts = 0;
    let lastError: any = null;
    try {
      while (attempts <= effectiveMaxRetires) {
        attempts++;
        if (params.arguments) {
          params.arguments.token = this.authenticationToken;
        } else {
          params.arguments = { token: this.authenticationToken };
        };
        try {
          if (!this.isConnected) {
            await this.connect();
          }
          const result = await this.client.callTool(
            params, resultSchema, options
          );
          this.lastActivityTime = new Date();
          return result;
        } catch (error) {
          lastError = error;
          console.error(`CallTool attempt ${attempts} failed:`, error);
          if (attempts > effectiveMaxRetires) {
            throw new Error(`Max retries reached from callTool: ${lastError.message || lastError}`);
          }
          await this.disconnect();
          await this.delayRetry(attempts);
        }
      }
      throw new Error('Unreachable code reached in callTool');
    } finally {
      if (this.isCallingWithTimeout) {
        this.isCallingWithTimeout = false;
      };
      this.lastActivityTime = new Date();
      this.startInactivityTimer();
    };
  };

  public async readResource(
    params: ReadResourceRequest["params"],
    options?: RequestOptions
  ): Promise<any> {
    if (this.verbose) {
      console.log(`params: ${JSON.stringify(params)}`);
    }
    if (!this.isConnected) {
      await this.connect();
    }
    let attempts = 0;
    let lastError: any = null;
    const effectiveMaxRetires = Math.max(1, this.jobMaxRetries);

    try {
      while (attempts < effectiveMaxRetires) {
        attempts++;
        try {
          if (!this.isConnected) {
            await this.connect();
          }
          const result = await this.client.readResource(params, options);
          this.lastActivityTime = new Date();
          if (this.verbose) {
            console.log(`result: ${JSON.stringify(result)}`);
          }
          if (!result || !result.contents) {
            throw new Error('No result from readResource');
          }
          const contents = result.contents;
          return contents;
        } catch (error) {
          lastError = error;
          if (attempts >= effectiveMaxRetires) {
            throw new Error(`Max retries reached from readResource: ${lastError.message || lastError}`);
          }
          await this.disconnect();
          await this.delayRetry(attempts);
        }
      }
      throw new Error('Unreachable code reached in readResource');
    } finally {
      this.lastActivityTime = new Date();
      this.startInactivityTimer();
    }
  };

  public async getPrompt(
    params: GetPromptRequest["params"],
    options?: RequestOptions
  ): Promise<any> {
    this.lastActivityTime = new Date();
    if (this.verbose) {
      console.log(`params: ${JSON.stringify(params)}`);
    }
    if (!this.isConnected) {
      await this.connect();
    }
    let attempts = 0;
    let lastError: any = null;
    const effectiveMaxRetires = Math.max(1, this.jobMaxRetries);

    try {
      while (attempts < this.jobMaxRetries) {
        attempts++;
        try {
          if (!this.isConnected) {
            await this.connect();
          }
          return await this.client.getPrompt(params, options);
        } catch (error) {
          lastError = error;
          if (attempts >= effectiveMaxRetires) {
            throw new Error(`Max retries reached from getPrompt: ${lastError.message || lastError}`);
          }
          await this.disconnect();
          await this.delayRetry(attempts);
        }
      }
      throw new Error('Unreachable code reached in getPrompt');
    } finally {
      this.lastActivityTime = new Date();
      this.startInactivityTimer();
    }
  };

  async fileUpload(
    file: File,
    fileType: string,
    userName: string,
    url?: string,
    endpoint: string = '/file/upload') {
    const connectionUrl = url || this.connectionUrl;
    if (connectionUrl === undefined) {
      throw new Error('No URL provided for connection');
    };
    const data = new FormData();
    data.append('file', file);
    data.append('fileType', fileType);
    data.append('userName', userName);

    try {
      const result = await axios.post(
        `${connectionUrl}${endpoint}`,
        data,
        {
          timeout: 100_000,
          headers: {
            'Access-Control-Allow-Origin': '*',
          }
        });
      this.lastActivityTime = new Date();
      if (result) {
        return result;
      }
    } catch (error) {
      console.error('Error during file upload:', error);
      throw error;
    }
  };

  fileCallback(
    blob: Blob,
    filename: string
  ) {
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', filename);
    document.body.appendChild(link);
    link.click();
    window.URL.revokeObjectURL(url);
  };

  async contentDownload(
    fileType: string,
    userName: string,
    fileUUID: string,
    endpoint: string = '/file/content/download'
  ) {
    const connectionUrl = this.connectionUrl;
    if (connectionUrl === undefined) {
      throw new Error('No URL provided for connection');
    };
    const data = new FormData();
    data.append('fileType', fileType);
    data.append('userName', userName);
    data.append('fileUUID', fileUUID);

    try {
      const response = await axios.post(
        `${connectionUrl}${endpoint}`,
        data,
        {
          responseType: 'blob',
          timeout: 100_000,

        }
      );
      this.lastActivityTime = new Date();
      if (response?.status === 200) {
        try {
          const blob = new Blob([response.data]);
          const contentDisposition = response.headers['content-disposition'];
          const filename = contentDisposition
            ? contentDisposition.split('filename=')[1]?.replace(/"/g, '')
            : `${fileUUID}_content.zip`;
          this.fileCallback(blob, filename);

        } catch (error) {
          console.error('Error parsing download response:', error);
        }
      }
    } catch (error) {
      console.error('Error during content download:', error);
      throw error;
    }
  }


  // async testPostCall(
  //   message: string, 
  //   url?: string, 
  //   endpoint: string = '/file/upload') {
  //   const connectionUrl = url || this.url;
  //   if (connectionUrl === undefined) {
  //     console.error('No URL provided for connection');
  //     return;
  //   };
  //   const data = new FormData();
  //   data.append('file', message);

  //   const result = await axios.post(
  //     `${connectionUrl}${endpoint}`, 
  //     data,
  //     {
  //     timeout: 100_000,
  //     headers: {
  //       'Access-Control-Allow-Origin': '*',
  //     }
  //   }
  //   );
  //   if (result) {
  //     return result;
  //   }
  // };
};


export type { Notification };
export default MCPClient;