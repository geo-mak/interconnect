Interconnect is a collection of connectivity primitives and interfaces for constructing inter-process communication pathways.

The RPC aspect is the main focus, and the main area of development.

## Architectural Overview.
```
┌─────────────────────────────────────────────┐
│                Service Layer                │
│     (Method Handlers, Extensions, etc.)     │
├─────────────────────────────────────────────┤
│                Session Layer                │
│         (Client, Server, Policies)          │
├─────────────────────────────────────────────┤
│               RPC Stream Layer              │
│         (Framing, Encoding/Decoding)        │
├─────────────────────────────────────────────┤
│             RPC Capability Layer            │
│    (Specs Negotiation, Establishing etc.)   │
├─────────────────────────────────────────────┤
│               Transport Layer               │
│             (TCP / Unix Sockets)            │
└─────────────────────────────────────────────┘
```

