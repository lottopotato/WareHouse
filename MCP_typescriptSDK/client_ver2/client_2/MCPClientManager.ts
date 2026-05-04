import { makeAutoObservable } from "mobx";
import { v4 as uuidv4 } from "uuid";

import MCPClient from "./MCPClient";
import type { Notification } from './MCPClient';
import type { MCPClientConfig, ClientInfo, ClientOptions } from "./Interface";

const DEFAULT_CLIENT_INFO: ClientInfo = {
  name: "DefaultClient",
  version: 'v1.0.0'
};

const DEFAULT_CLIENT_OPTIONS: ClientOptions = {
  capabilities: {
    tools: {},
    resources: {},
    prompts: {}
  }
};

class MCPClientManager {
  private clients: Map<string, MCPClient>;
  public verbose: boolean;

  constructor(verbose: boolean = false) {
    makeAutoObservable(this);
    this.clients = new Map<string, MCPClient>();
    this.verbose = verbose;
  }

  public newClient(
    config: Omit<MCPClientConfig, 'clientId'> & { clientId?: string },
    authenticationToken: string | undefined,
    onNotification?: (notification: Notification) => void,
    verbose: boolean = false,
    connectionTimeout?: number,
  ): MCPClient {
    const clientId = config.clientId ?? uuidv4();
    // Merge provided clientInfo with defaults
    const clientInfo: ClientInfo = {
      ...DEFAULT_CLIENT_INFO,
      ...config.clientInfo,
    };

    // Merge provided clientOptions with defaults, handling nested capabilities
    const clientOptions: ClientOptions = {
      ...DEFAULT_CLIENT_OPTIONS,
      ...config.clientOptions,
      capabilities: {
        ...DEFAULT_CLIENT_OPTIONS.capabilities,
        ...(config.clientOptions?.capabilities || {}),
      },
    };
    const fullConfig: MCPClientConfig = {
      ...config,
      clientId,
      clientInfo,
      clientOptions
    };

    if (this.clients.has(fullConfig.clientId) && this.verbose) {
      throw new Error(`Client with ID '${fullConfig.clientId}' already exists.`);
    }
    const client = new MCPClient(
      fullConfig, 
      authenticationToken,
      verbose, 
      onNotification ? onNotification : undefined,
      connectionTimeout ? connectionTimeout : undefined
    );
    this.clients.set(fullConfig.clientId, client);
    return client;
  };

  public getClients(): Map<string, MCPClient> {
    return this.clients;
  };

  public getClient(clientId: string): MCPClient | undefined {
    return this.clients.get(clientId);
  };

  public dropClient(clientId: string): boolean {
    if (this.clients.has(clientId)) {
      this.clients.get(clientId)?.disconnect();
    }
    return this.clients.delete(clientId);
  };

  public async shutdownAllClients(): Promise<void> {
    if (this.verbose) {
      console.log('shutting down all managed clients...');
    };
    const disconnectPromises: Promise<void>[] = [];
    for (const client of this.clients.values()) {
      disconnectPromises.push(client.disconnect());
    }
    await Promise.all(disconnectPromises);
    this.clients.clear();
    if (this.verbose) {
      console.log('All managed clients have been shut down.');
    }
  };

  public getClientIds(): string[] {
    return Array.from(this.clients.keys());
  };
};

export default MCPClientManager;