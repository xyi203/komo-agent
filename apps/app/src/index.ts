// Public surface of the shared komo renderer. Each host (Electron renderer, web
// bootstrap) builds an `HttpKomoClient` over its own gateway resolver, installs
// it with `installHost`, then mounts `<KomoApp/>`. The host also imports the
// stylesheet — see apps/desktop/src/renderer/styles.css.

export { KomoApp } from "./app/KomoApp";
export { HttpKomoClient } from "./shared/api/client";
export { installHost } from "./shared/bootstrap";

// Browser-only: the key-entry screen and where its endpoint is stored.
export { ConnectGate } from "./features/connect/ConnectGate";
export { consumeQueryParams, currentGateway } from "./features/connect/gateway-storage";

export type { Gateway, GatewayResolver, KomoClient, KomoConnectResponse } from "./shared/api/types";
export type { HostTag } from "./shared/api/runtime";
