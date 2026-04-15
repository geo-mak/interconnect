Interconnect is a modular framework for constructing inter-process communication pathways.

Interconnect's design aims at providing OS-level framework for bridging the interaction between user-space applications, services and devices, local and remote alike, where "everything" is **neither** an **object** nor a **file**, but what it actually is, and what it declares in terms of functionalities exposed directly via well-defined bindings. 

> **Note**: The design details apply to the reference implementation in **Rust**. Implementations in other languages may diverge according to the capabilities and limitations of the implementation language.

## Architecture and general design approach

Interconnect has a dataflow-oriented architecture where data availability drives computation.

Interconnect's design is layered with modular components and makes heavy use of static parametric polymorphism.

From design perspective, components are **viewed** as **micro-servers** offering **services** that can be static, dynamic, local and remote.

Interconnect's design favours variety of optimized components over common ones with complex configurations in general.
Diversity with optimized internals and simple setup is considered a better strategy than uniformity with wide set of configuration options.

Interconnect's main layers are:

**Transport Layer**:
Transport components implement the actual mechanics of delivering data from I/O devices to decoders, and from encoders to I/O devices.

Transport components are viewed **and** implemented as transport protocols in their own right, where each implementation has its own specifications and semantics for establishing connections.

From design perspective, the underlying device, technology or networking stack used by the transport model is considered implementation detail, because it doesn't play role in its identity as a transport type.

Transport models will not be identified as IP, UDS or something else, because these are just internals, even if they are mentions in the documentation. For instance two transport models may utilize IP, but they might have very different features and with different protocol for establishing connections.

Moreover, I/O abstractions are somehow "clunky" and a major source of bugs and inefficiency, because they tell too little about the underlying mechanics with a lot of redundant buffering and data-copying in the chain.

The implementation of transport components aims at abstracting the underlying I/O mechanisms with high level of transparency about the allocation details and buffering strategy.

Each transport model offers optimizations and tradeoffs for particular use-case.

Designing and implementing transport models is an **essential** part of the project, where new transport components may get
added.

**Session Layer**: 
Session layer provides components that manage the session's state and perform dispatch according to that state.

The provided components are role-based like server and client.

That actual implementation of these types may vary in terms of multiplexing-capabilities and efficiency.

The design takes into account the various needs and their tradeoffs in terms of multiplexing-capabilities and the required
resources.

**Message Layer**:
Provides components for encoding, decoding and validation of messages, when sending and receiving.

Message layer implements the machinery to safely and correctly encode and decode the **defined message** for the **target method**.

By default, messages are passed to the interface carrying borrowed data, and get returned after receiving carrying borrowed data.

Thanks to the custom-layout and strict alignment rules, all types in a message can be accessed borrowed without conversion to owned types and with the lifetime-bound as the only restriction applied, something that would be very limited, not possible or recklessly unsafe in the "naïve" common world of encoding and decoding out there.

However, received messages allow conversion to owned types when borrowing can be restrictive.

**Service Layer**:
Service layer serves typing and runtime-configurations where the user-code constructs messages, and makes use of the received messages using the defined types and the implementation of the service's interfaces as a "thin" layer on top of the other layers.

## Data exchange
Interconnect's unit of exchange is "message".

Messages are exchanged in binary format with **untagged** data representation.

Interconnect defines its own data model that describes the byte-patterns of exchange-types and the layout of messages.

Interoperability is achieved by adhering to the ABI (Application Binary Interface).

Interconnect requires native implementation of its type-system and its associated components like encoders and decoders 
in order to send and receive messages that conform to its data model.

The native implementation of the type-system and other support components like the transport components and the endpoints' 
implementations are referred to as the "runtime library".

The runtime-library provides components to construct a compliant implementation of Interconnect.

The specifications and the details of the data model are described in-depth in "SPECS" file and updated regularly.

Moreover, Interconnect defines its own SDL (Service Definition Language) and provides toolchain like compiler and code-generation backends for generating integrated endpoints that conform to the user-defined interface.

The SDL and its toolchain are an **usability and safety** option for making use of Interconnect in an automated manner.

The architecture of the compiler and other details related to the toolchain are not published yet.

## Async, concurrency and parallelism

Within the context of this project, these terms are understood as defined:

- Async: An event-based execution model, where the control flow can switch to a notification broker,
  instead of continuously checking the availability of a resource.
  This term could be viewed as a "language-abuse" to describe a non-blocking, "cooperative" execution model,
  but it is commonly used out there, so this project assigns the above definition to it only.

- Concurrency: An execution model, where multiple instructions' streams **may** take place at the same time.
  Concurrency can be truly parallel by utilizing multiple processing elements at the same time, or partial by means of sharing
  a processing element in time-slices (context switching).

- Parallelism: The ability of a particular system to operate multiple instances of its processing elements at the same time.

Concurrency and parallelism are synonymous within the context of this project, because the programming model
must assume parallelism to enure sound access to data stores and the proper data transformation.

Interconnect utilizes "async" for implementing its data-flow model, because this model is inherently "async", 
or the other way around, "async" has by-definition a data-flow execution model.

## Error handling and panic policy

From design perspective (many implementations are still more or less "prototypes"):

- Everything that can fail at runtime has to return an error.

- Everything that is not expected to fail at runtime should crash the process when it does.

The panic-policy is to abort. The reasons for adopting abort instead of unwinding are: 
- Unwinding has a very high overhead (if not carefully/manually optimized).
- Maintaining the conceptual clarity of the control-flow.
- Designing for "unexpected" failure is an oxymoron.

Theoretically, unwinding can be used for implementing a very efficient error handling strategy, by means of reducing error checks in each call-frame and carefully designed "catch-points" in the call chain, but this model is less flexible and very tricky to setup and maintain properly, especially across refactorings.

Interconnect's design differentiates between control-flow errors and error-reporting with two separate types:
- Constrained error-type.
- Reporter instance providing access to the reporting subsystem.

Returned errors serve **informing** the calling context which path to take after an error has been encountered, 
with enough information to serve as **branching flags**.

Error-reporting is performed via a reporting subsystem exposed via a reporting-component, that produces reports with certain structure and format. Depending on the reporting-component used, reporting can add a non-trivial overhead to the system if utilized without careful consideration, therefore reporting is considered a privileged capability that shall be given transparently.

## Memory allocation

Memory management is a difficult thing to implement without compromises and where design choices are accompanied with tradeoffs.

The common approach is to let components allocate memory unconstrained on-demand at multiple places via the global general-purpose allocator, and to deallocate that memory when the work with it ends. This common approach is very flexible but it has major disadvantages like:
- It is inefficient in terms of performance.
- It is non-deterministic in terms of failure points.
- It is non-transparent in terms of allocation points.
- It leads to high memory fragmentation.

Memory management is a major performance and stability factor, therefore Interconnect's design embraces determinism and transparency regarding dynamic memory-allocation to a larger degree while allowing exceptions to take place. 

From design perspective, memory is considered a **service**, provisioned via **memory servers**.

Components ask for memory from an explicitly passed memory server that may provide a block of memory which can be accessed, shared and managed as **protocoled service**. Each component or layer can define its own requirements in terms of protocols required to access that memory.

The core idea is to enable sharing and reusability of the same acquired memory across components and layers as much as possible.

Interconnect's design is **centered** around memory subsystems and their implementation for various cases is an **essential** part of the project.