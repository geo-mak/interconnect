Interconnect is a collection of connectivity primitives and interfaces for constructing inter-process communication pathways.

The RPC aspect is currently the main focus, and the main area of development.

## Main Layers Overview.
```
┌─────────────────────────────────────────────┐
│              Application Layer              │
│   (Operations' Handlers, Extensions, etc.)  │
├─────────────────────────────────────────────┤
│                Session Layer                │
│         (Client, Server, Policies)          │
├─────────────────────────────────────────────┤
│                 Stream Layer                │
│     (Framing, Encryption, Encoding etc.)    │
├─────────────────────────────────────────────┤
│               Connection Layer              │
│    (Specs Negotiation, Establishing etc.)   │
├─────────────────────────────────────────────┤
│                Transport Layer              │
│           (TCP, Unix Sockets etc.)          │
└─────────────────────────────────────────────┘
```