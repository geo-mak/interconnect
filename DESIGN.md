Interconnect is a modular framework for constructing inter-process communication pathways.

Interconnect's design aims at providing OS-level framework for bridging the interaction between user-space applications, services and devices, local and remote alike, where "everything" is **neither** an **object** nor a **file**, but what it actually is, and what it declares in terms of functionalities exposed directly via well-defined bindings. 

## Architectural model and implementation highlights

Interconnect has dataflow oriented architecture where data availability drives computation.

Interconnect's runtime is layered with modular components and makes heavy use of static parametric polymorphism.

The main layers are:

**Transport Layer**:
Transport components implement the actual mechanics of delivering data from I/O devices to decoders, and from encoders to I/O devices.

Each transport components is viewed **and** implemented as a fully-fledged transport-protocol, with its own transport-model, and its own
specifications and semantics for establishing connections.

From design perspective, the underlying device, technology or networking stack used by the transport model is considered implementation detail, because it doesn't play role in its identify as a transport type.

Transport models will not be identified as TCP, UDS or something else, because these are just internals, even if they are mentions in the documentation. 

For instance two transport models may utilize TCP, but they might have very different features and with different protocol for establishing
connections, so saying that the transport is "TCP" would say "too little" about it.

Each transport model offers optimizations and tradeoffs for particular use-case.

Designing and implementing transport models is an essential part of the project, where new transport components may get
added.

**Session Layer**: 
Session layer provides the components that store the session-state and perform dispatch according to that state.

The provided components are role-based like server and client.

That actual implementation of these types may vary in terms of multiplexing-capabilities and efficiency.

The design takes into account the various needs and their tradeoffs in terms of multiplexing-capabilities and the required
resources.

**Message Layer**:
Provides components for encoding, decoding and validation of messages, when sending and receiving.

Message layer exposes types the application can use to construct a **correct message** for the **target method**, and 
enables efficient and safe zero-copy encoding and decoding of messages.

Messages are passed to the interface carrying borrowed data, and get returned after receiving carrying borrowed data.

Thanks to the custom-layout and strict alignment rules, all types in a message can be accessed in borrowed form, without restrictions other than the lifetime-bound, something that would be very limited, not possible or recklessly unsafe in the "naïve" common 
world of encoding and decoding out there.

**Application Layer**:
Application layer is where the user-code constructs messages and makes use of the received messages. This layer is external to the project in terms of implementation.

Interconnect uses this layer to model various use-cases for better understandability and for providing more support types and options where needed.

## Async, concurrency and parallelism

Within the context of this project, these terms are understood as defined:

- Async: An event-based execution model, where the control flow of the thread can switch to a notification broker,
  instead of entering a loop for continuously checking the availability of a resource.

- Concurrency: An execution model, where multiple instructions' streams **may** take place at the same time.
  Concurrency can be "truly" parallel by running multiple cores/execution units, or partial by means of sharing
  the resource in time slices (context switching).

- Parallelism: The ability of a particular system to operate multiple instances of its execution units at the same time.

Concurrency and parallelism are synonymous within the context of this project, because the programming model
must assume parallelism to enure sound access to data stores and the proper data transformation.

Interconnect utilizes "async" for implementing its data-flow model, because this model is inherently "async", 
or the other way around, "async" has by-definition data-flow model.

## Error handling and panic policy

From design perspective (many implementations are still more or less "prototypes"):

- Everything that can fail at runtime has to return an error.

- Everything that is not expected to fail at runtime should crash the process when it does.

The panic-policy is to abort. The reasons for adopting abort instead of unwind are: 
- "unwinding" has a very high overhead (if not carefully/manually optimized).
- Maintaining the conceptual clarity of the control-flow.
- Designing for "unexpected" failure is an oxymoron.

Interconnect's design differentiates between control-flow errors and error-reporting with two separate types:
- Constrained error-type.
- Reporter instance providing access to the reporting sub-system.

Returned errors serve **informing** the calling context which path to take after an error has been encountered, 
with enough information to serve as **branching flags**.

Error-reporting is performed via the reporting sub-system, that produces reports with certain structure and format, 
for machines and/or humans.

## Data exchange
Interconnect's unit of exchange is "message".

Messages are exchanged in binary format with **untagged** data representation.

Interconnect utilizes ABI (Application Binary Interface) for data representation, where interoperability is achieved by adhering 
to the ABI.

Interoperability and compatibility strategy at the ABI-level is still **undetermined**. 

The current implementation enables version-detection and multi-version support for both the specification protocol and the ABI.
Each connection starts by announcing the version of its specification protocol and its ABI, which will be used for the entire session 
(no per-message versioning), but it is still experimental.

Since the ABI is a shared "language" between systems, interoperability and stability are of a strategic importance.

The current prototype I am working on has the following design aspects:
- IPC definition language with type system (numbers, enums, structs, interfaces..etc).
- Compiler and libraries for generating language-specific ABI-compatible implementations from IPC-definition files.
- Support generating "async" APIs.
- Reuseable linear memory for encoding and decoding.
- Compiling to standardized format that acts as intermediate representation for building native runtimes and code-generators (Bring your own bottle and welcome to the party).

These design aspects aim at abstracting the details of the ABI using higher-level representation of the API, for 
automated management of interoperability and compatibility.

There is already a (kind of) working implementation of the definition language and its type system in terms of 
compiler and native-support types (for Rust), but their implementation is fragile and not yet ready to show up.