Interconnect is a collection of connectivity primitives and interfaces for constructing inter-process communication pathways.

The RPC aspect is currently the main focus, and the main area of development.

## Main layers overview.
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

## Architectural model and implementation highlights

Interconnect has dataflow oriented architecture where data availability drives computation.

The entire system is modeled as a set of "functional units" that can be similar or heterogenous.

Functional units are specialized processing units that encapsulate "control flow".

Functional units may have exclusive data stores or operate on shared data stores.

Statically sized types with static memory allocation are the main way of creating data stores.

## Async, concurrency and parallelism

Within the context of this project, these terms are understood as defined:

- Async: An event-based execution model, where the control flow of the thread can switch to a notification broker,
  instead of entering a loop for continuously checking the availability of a resource.

- Concurrency: An execution model, where multiple instructions' streams **may** take place at the same time.
  Concurrency can be "truly" parallel by running multiple core/execution units, or partial by means of sharing
  the resource in time slices (context switching).

- Parallelism: The ability of a particular system to operate multiple instances of its execution units at the same time.

Concurrency is treated like parallelism within the context of this project, because the programming model
must assume parallelism to enure sound access to data stores and the proper data transformation.

The challenges and complexities of these models are inherent to the dominant **"Von Neumann"** hardware architecture,
where the "control flow" drives computation, and problems like multiple "runtimes" (CPU scheduler, OS scheduler, Process scheduler), memory ordering, synchronizations..etc are just a byproduct.