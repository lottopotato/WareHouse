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

class MCPClient {
  private readonly id: string;
  private readonly client: Client;
  private readonly connectionUrl: string;
  private readonly authenticationToken: string;
  private transport: StreamableHTTPClientTransport;
  private sessionId: string | undefined;
  private onNotification?: (notification: Notification) => void;
  public isConnected: boolean = false;
  public jobMaxRetries: number;
  public verbose: boolean;
  public connectionTimeout: number = 300;
  public lastActivityTime: Date = new Date();
  public inactivityTimer: any;

  constructor(
    config: MCPClientConfig, 
    authenticationToken: string,
    verbose: boolean = false,
    onNotification?: (notification: Notification) => void,
    connectionTimeout?: number,
  ) {
    this.id = config.clientId;
    this.authenticationToken = authenticationToken;
    this.client = new Client(
      config.clientInfo, config.clientOptions
    
    );
    this.transport = new StreamableHTTPClientTransport(
      new URL(`${config.connectionUrl}/mcp`)
    );
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
    if (connectionTimeout) {
      this.connectionTimeout = connectionTimeout;
    }
  };

  private startInactivityTimer() {
    this.inactivityTimer = setInterval(() => {
      const now = new Date();
      const timeSinceLastActivity = now.getTime() - this.lastActivityTime.getTime();
      if (timeSinceLastActivity > this.connectionTimeout) {
        this.disconnect();
      }
    }, 1000*30); // Check every 30 seconds
  };

  private clearInactivityTimer() {
    clearInterval(this.inactivityTimer);
  };

  public getInformation() {
    console.log(this.client)
    if (this.transport) console.log(this.transport)
  };

  public async connect(): Promise<void> {
    if (this.isConnected) {
      return;
    }
    if (this.verbose) {
      console.log(`MCPClient '${this.id}' attempting to connect via ${this.transport}...`);
    }
    if (this.client && !this.client.transport) {
      await this.disconnect(); // Ensure clean state
      this.transport = new StreamableHTTPClientTransport(
        new URL(`${this.connectionUrl}/mcp`)
      );
    };
    await this.client.connect(this.transport);
    if (this.transport.sessionId) {
      this.sessionId = this.transport.sessionId;
    }
    this.isConnected = true;
    if (this.verbose) {
      console.log(`MCPClient '${this.id}' connected (placeholder).`);
    }
    this.startInactivityTimer();
  };

  public async connectTest() : Promise<string | null | undefined> {
    const clientVerbose = this.verbose;
    this.verbose = true;
    if (!this.isConnected) {
      await this.connect()
    };
    this.verbose = clientVerbose;
    if (this.transport && 'sessionId' in this.transport) {
      return this.transport.sessionId;
    };
    await this.disconnect();
    return null;
  }
  
  public async disconnect(): Promise<any> {
    if (!this.isConnected) {
      return;
    }
    if (this.verbose) {
      console.log(`MCPClient '${this.id}' disconnecting...`);
    }
      try {
        const result = await this.client.callTool({
          name: 'exclude_server_instance',
          arguments: { 
            token: this.authenticationToken, 
            sessionId: this.sessionId
          }
        });

        await this.client.close();
        this.isConnected = false;
        this.sessionId = undefined;
        if (this.verbose) {
          console.log(`MCPClient '${this.id}' disconnected.`);
        }
        this.clearInactivityTimer();
        if (result) {
          return result;
        }
      } catch {
        // await this.connect();
      } finally {
      }

      return;
  };

  public async callTool(
    params: CallToolRequest["params"],
    resultSchema?: typeof CallToolResultSchema | typeof CompatibilityCallToolResultSchema,
    options?: RequestOptions
  ): Promise<any> {
    if (this.verbose) {
      console.log(`params: ${JSON.stringify(params)}`);
    }
    if (!this.isConnected) {
      await this.connect();
    }
    let attempts = 0;
    while (attempts <= this.jobMaxRetries) {
      if (params.arguments) {
        params.arguments.token = this.authenticationToken;
      } else {
        params.arguments = { token: this.authenticationToken };
      };
      try {
        return await this.client.callTool(
          params, resultSchema, options
        );
      } catch {
        await this.connect();
      } finally {
        attempts++;
      }
    }
    throw new Error('Max retries reached from callTool');
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

    while (attempts < this.jobMaxRetries) {
      try {
        const result = await this.client.readResource(params, options);
        if (this.verbose) {
          console.log(`result: ${JSON.stringify(result)}`);
        }
        if (!result) {
          throw new Error('No result from readResource');
        }
        const contents = result.contents;
        if (!contents) {
          throw new Error('No contents in readResource result');
        }
        return contents;
      } catch (error) {
        console.log(error)
        attempts++;
        await this.disconnect();
        await this.connect();
      }
      
    }
    throw new Error('Max retries reached from readResource');
  };

  public async getPrompt(
    params: GetPromptRequest["params"],
    options?: RequestOptions
  ): Promise<any> {
    if (this.verbose) {
      console.log(`params: ${JSON.stringify(params)}`);
    }
    if (!this.isConnected) {
      await this.connect();
    }
    let attempts = 0;

    while (attempts < this.jobMaxRetries) {
      try {
        return await this.client.getPrompt(params, options);
      } catch {
        attempts++;
        await this.disconnect();
        await this.connect();
      }
    }
    throw new Error('Max retries reached from getPrompt');
  };

  // async fileUpload(
  //   file: File,
  //   fileType: string,
  //   userName: string,
  //   apiKey?: string,
  //   url?: string, 
  //   endpoint: string = '/file/upload') {
  //   const connectionUrl = url || this.url;
  //   if (connectionUrl === undefined) {
  //     console.error('No URL provided for connection');
  //     return;
  //   };
  //   const data = new FormData();
  //   data.append('file', file);
  //   data.append('fileType', fileType);
  //   data.append('userName', userName);
  //   if (apiKey) {
  //     data.append('apiKey', apiKey);
  //   }
    
  //   const result = await axios.post(
  //     `${connectionUrl}${endpoint}`, 
  //     data,
  //     {
  //     timeout: 100_000,
  //     headers: {
  //       'Access-Control-Allow-Origin': '*',
  //     }
  //   });
  //   if (result) {
  //     return result;
  //   }
  // };


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