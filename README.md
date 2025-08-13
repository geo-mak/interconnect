# Autonomic Interconnect

High performance remote procedure call protocol.

> **⚠️ This project is in early development stage.**

## Features
- Asynchronous with bidirectional communication.
- Layered extensible architecture with high efficiency abstractions and clear semantics.
- Lightweight message format.
- Out-of-the-box support for TCP and Unix domain sockets.

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