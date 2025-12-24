Interconnect is a modular framework for constructing inter-process communication pathways.

Interconnect's design aims at providing OS-level framework for bridging the interaction between user-space applications, services and devices, local and remote alike, where "everything" is **neither** an **object** or a **file**, but what it actually is, and what it declares in terms of functionalities exposed directly via well-defined bindings. 

## Runtime model overview.
```
┌─────────────────────────────────────────────┐
│              Application Layer              │
│   (Operations' Handlers, Extensions, etc.)  │
├─────────────────────────────────────────────┤
│                Session Layer                │
│     (Client, Server, Routing, Policies)     │
├─────────────────────────────────────────────┤
│                Message Layer                │
│            (Encoding/Decoding etc.)         │
├─────────────────────────────────────────────┤
│               Connection Layer              │
│    (Specs Negotiation, Establishing etc.)   │
├─────────────────────────────────────────────┤
│                Transport Layer              |
│    (Framing, Encryption, Flow Control etc.) │
└─────────────────────────────────────────────┘
```

Note:
The connection-Layer will get merged with transport-layer, where establishing a connection is the 
responsibility of the selected transport-model, according to its semantics.

That's to say that the current specification protocol will be deprecated as transport-agnostic mechanism 
of establishing connections, in favour of transport-defined mechanisms.

This will give the transport-model fine-grained optimized control over establishing connections according 
to its features and threat-model.

This change in motivated by the fact that new transport-models will be implemented where the
"remote" byte-stream oriented transport-models (TCP..etc.) are available in addition to other models
that are local and based on protected shared-memory.

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
  Concurrency can be "truly" parallel by running multiple cores/execution units, or partial by means of sharing
  the resource in time slices (context switching).

- Parallelism: The ability of a particular system to operate multiple instances of its execution units at the same time.

Concurrency and parallelism are synonymous within the context of this project, because the programming model
must assume parallelism to enure sound access to data stores and the proper data transformation.

The challenges and complexities of these models are inherent to the dominant **"Von Neumann"** hardware architecture,
where the "control flow" drives computation, and problems like multiple "runtimes" (CPU scheduler, OS scheduler, Process scheduler), memory ordering, synchronizations..etc are just a byproduct.

## Notes on safety
Many Rust's projects tend to brag for not using "unsafe" as if is the "brand" of safety. 

This project doesn't subscriber to such "ethos", and doesn't shy away from utilizing "unsafe" mechanisms for achieving goals.

On reason for this is relying on custom data structures in many places, which require direct access to memory.

Another reason is the "syntactic inability" to infer lifetimes and access patterns in particular cases.

Rust offers great safety features provided by its rich type system and rich syntactic rules with variety of  
data structures in the standard library that improve "accessibility" to a wider audience. 
All of this while not dumping-down the language and without sacrificing the low-level control.

It is also one of the easiest languages to learn, if we "ignore" the "bad culture" that introduces people to programming by dumped-down, inefficient and messed-up languages, where people "mistakenly" think that data-pieces have parents, children, aunts, uncles..other siblings, and they all can be returned from any function anywhere, because they all live in the kingdom of heap, near the creator..and the destructor, the so called GC.

So, such features make Rust one of the "rare" programming systems that are accessible and scalable, especially today,
where computer-systems are ubiquitous and challenged by demanding tasks and efficiency requirements.

But, I think the "safety" ethos is both, troublesome and uninspiring.

- Troublesome: "safe" rust implies using off-the-shelf data structures, that are both limited and opinionated.
It also implies assuming "speculatively" optimization patterns that are neither well documented, nor guaranteed by any "specs".
There are no "zero-cost" abstractions, unless there are well-defined "compile-time" evaluation patterns for such things.
So the "non-zero-cost" abstractions are always a safe bet, where choices in that regard are simply tradeoffs based on the effect of problem-space and the "willingness" to accept them.

- Uninspiring: "safety" is an essential property of systems people can rely on, but many systems will never see life if they are guided by the ethos of "safety", because such ethos undermines "creativity" and joy of "making" things. 
Fundamentally, there is no problem to solve, we will all die anyway! Life is all about approaching challenges with "creativity", so 
it is unnecessary burden to make it a sort of a "miserable fight".

As a final note, whether you like what I do or not, I hope I could have made a case for "creativity" as the right ethos to promote,
where this "creativity" can rely on a well-designed programming system, with compile-time analysis for "sanity-checks".

## Error handling and panic policy

From design perspective (many implementations are still more or less "prototypes"):

- Everything that can fail at runtime has to return an error.

- Everything that is not expected to fail at runtime should crash the process when it does.

The panic-policy is to abort. The reasons for adopting abort instead of unwind are: 
- "unwinding" has a very high overhead (if not carefully/manually optimized).
- Maintaining the conceptual clarity of the control-flow.
- Designing for "unexpected" failure is an oxymoron.

In simple terms, guarding execution against unexpected "things" is either wrapping possibly "misbehaving" code **external** 
to the system with "isolated effect" (e.g OS with processes), or a messed-up implementation of the system itself that lacks 
proper design and **system analysis**.

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
- IPC definition language with type system (numbers, enums, structs, traits..etc).
- Compiler and libraries for generating language-specific ABI-compatible implementations from IPC-definition files.
- Support generating "async" APIs.
- Reuseable linear memory with virtual offsets for encoding and decoding.
- Useful defaults with customizations options.

These design aspects aim at abstracting the details of the ABI using higher-level representation of the API, for 
automated management of interoperability and compatibility.

There is already a (kind of) working implementation of the definition language and its type system in terms of 
compiler and native-support types (for Rust), but their implementation is fragile and not yet ready to show up.