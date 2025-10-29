# Interconnect

High performance remote procedure call protocol.

> **⚠️ This project is in early development stage.**

## Features
- Layered extensible architecture with high efficiency abstractions and clear semantics.
- Lightweight message format.
- Out-of-the-box support for TCP and Unix domain sockets.

This project aims at providing the best possible usability and performance compared to an application-specific protocol.

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