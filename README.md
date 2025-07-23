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
│               Service Layer                 │
│     (Method Handlers, Extensions, etc.)     │
├─────────────────────────────────────────────┤
│              Connection Layer               │
│      (Listeners, Roles (Client / Server))   │
├─────────────────────────────────────────────┤
│              RPC Stream Layer               │
│         (Framing, Encoding/Decoding)        │
├─────────────────────────────────────────────┤
│             Raw I/O Stream Layer            │
│             (TCP / Unix Sockets)            │
└─────────────────────────────────────────────┘
```