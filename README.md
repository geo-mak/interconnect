# Autonomic Interconnect

High performance remote procedure call protocol.

> **⚠️ This project is in early development stage.**

## Features
- No clunky interface generation or schema definitions: a schema is any encodable type, and Rust’s compile-time monomorphization is used to specialize interfaces.
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
│          Connection Management Layer        │
│     (Listeners, Roles (Client / Server))    │
├─────────────────────────────────────────────┤
│               RPC Stream Layer              │
│         (Framing, Encoding/Decoding)        │
├─────────────────────────────────────────────┤
│             RPC Capability Layer            │
│    (Versioning, Feature Negotiation, etc.)  │
├─────────────────────────────────────────────┤
│             Raw I/O Stream Layer            │
│             (TCP / Unix Sockets)            │
└─────────────────────────────────────────────┘
```