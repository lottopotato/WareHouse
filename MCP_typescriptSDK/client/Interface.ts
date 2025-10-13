interface ClientInfo {
  name: string,
  version: string,
  title?: string;
  [x: string]: unknown;
};

interface ClientCapabilities {
  tools: Record<string, any>;
  resources: Record<string, any>;
  prompts: Record<string, any>;
  [key: string]: any;
};

interface ClientOptions {
  capabilities: ClientCapabilities;
};

interface MCPClientConfig {
  clientId: string;
  connectionUrl: string;
  clientInfo: ClientInfo;
  clientOptions: ClientOptions;
};

export type { ClientInfo, ClientCapabilities, ClientOptions, MCPClientConfig };