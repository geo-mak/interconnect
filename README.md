# Autonomic Interconnect

High performance remote procedure call protocol.

> **⚠️ This project is currently under construction.**

## Features

- Asynchronous with bidirectional communication.
- Layered extensible architecture with high efficiency abstractions and clear semantics.
- Lightweight message format.
- Out-of-the-box support for TCP and Unix domain sockets.

## Architectural Overview.
```
┌─────────────────────────────────────────────┐
│              Service Layer                  │
│    (Method Handlers, Extensions, etc.)      │
├─────────────────────────────────────────────┤
│           Connection State Layer            │
│             (Client / Server)               │
├─────────────────────────────────────────────┤
│              RPC Stream Layer               │
│         (Framing, Encoding/Decoding)        │
├─────────────────────────────────────────────┤
│              I/O Stream Layer               │
│            (TCP / Unix Sockets)             │
└─────────────────────────────────────────────┘
```