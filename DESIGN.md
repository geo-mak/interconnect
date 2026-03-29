Interconnect is a modular framework for constructing inter-process communication pathways.

Interconnect's design aims at providing OS-level framework for bridging the interaction between user-space applications, services and devices, local and remote alike, where "everything" is **neither** an **object** nor a **file**, but what it actually is, and what it declares in terms of functionalities exposed directly via well-defined bindings. 

## Architectural model and implementation highlights

Interconnect has a dataflow-oriented architecture where data availability drives computation.

Interconnect's design is layered with modular components and makes heavy use of static parametric polymorphism.

The main layers are:

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

Thanks to the custom-layout and strict alignment rules, all types in a message can be accessed borrowed without conversion to owned types and with the lifetime-bound as the only restriction applied, something that would be very limited, not possible or recklessly unsafe in the "naïve" common 
world of encoding and decoding out there.

However, received messages allow conversion to owned types when borrowing can be restrictive.

**Application Layer**:
Application layer serves typing and runtime-configurations where the user-code constructs messages, and makes use of the received messages using the defined types and the implementation of the application's interfaces as a "thin" layer on top of the other layers.

Interconnect uses this layer also to model various use-cases for better understandability and for providing more support types and options where needed.

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

Moreover, Interconnect defines its own IDL (IPC Definition Language) and provides toolchain like compiler and code-generation backends for generating integrated endpoints that conform to the user-defined interface.

The IDL and its toolchain are an **usability and safety** option for making use of Interconnect in an automated manner.

The architecture of the compiler and other details related to the toolchain are not published yet.

## Async, concurrency and parallelism

Within the context of this project, these terms are understood as defined:

- Async: An event-based execution model, where the control flow can switch to a notification broker,
  instead of entering a loop for continuously checking the availability of a resource.
  This term could be viewed as a "language-abuse" to describe a non-blocking, "cooperative" execution model,
  but it is commonly used out there, so this project assigns the above definition to it only.

- Concurrency: An execution model, where multiple instructions' streams **may** take place at the same time.
  Concurrency can be "truly" parallel by running multiple processing elements, or partial by means of sharing
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

The common approach is to let components allocate memory unconstrained on-demand at multiple places via the global general-purpose allocator, and to deallocate that memory when the work with it ends. This common approach approach is:
- Inefficient in terms of performance.
- Non-Deterministic in terms of failure points.
- Non-Transparent in terms of allocation points.
- Leads to high memory fragmentation.

Memory management is a major performance and stability factor, therefore Interconnect's design embraces determinism and transparency regarding dynamic memory-allocation to a larger degree while allowing exceptions to take place. 

From design perspective, memory is considered a **service**, provisioned via **memory servers**.

Components ask for memory from an explicitly passed memory server that may provide a block of memory which can be accessed, shared and managed as **protocoled service**. Each component or layer can define its own requirements in terms of protocols required to access that memory.

The core idea is to enable sharing and reusability of the same acquired memory across components and layers as much as possible.

Interconnect's design is **centered** around memory subsystems and their implementation for various cases is an **essential** part of the project.

## AI usage and policy
Since I started tinkering with the so-called "AI", namely "LLMs", I was both impressed and skeptical, 
impressed for what has been achieved and skeptical of what it could be done with it compared to the "hype" surrounding this technology.

To make this section short and straightforward, I have used this technology in the context of software development exclusively for:

- Prototyping of designs where code quality or strict correctness doesn't matter compared to the effort of evaluating a strategy in general.

- Prototyping of unit-tests according to a pre-exiting pattern, which I end up later refactoring and polishing manually.

- Codebase "research" for building a picture of a particular solution implemented in other codebases that forms a multi-file, multi-package puzzle in large codebases with the intent of narrowing the search-scope with various accuracy instead of getting lost in details of that codebase for days.

- Review of code I wrote when I was tired, and where any review could be better than nothing, even if the suggestions would be inaccurate or flat out nonsense.

I presume that this technology is here to stay as part of what could be called "smart" IDE, but I don't think it is important in the making of any serious software that people could rely on for these **main** reasons:

- It is inherently faulty in a domain where a trivial mistake could mean disasters or deaths.
- Writing code is not the challenge (at least in my case), most of my time is spent in design and finding optimization's tricks.
- Maintaining a codebase requires understanding its inner working in its entirety.
- Generating a pile to get it "polished" later is a naïve and misguided approach. Refactoring a foreign codebase is more time-consuming than writing from scratch.
- Distractive to crisp thinking, where a vivid attention must be given.
- Induces overconfidence and false sense of achievement.

So the reality is not black or white, it is actually very colorful, but the overhype and the unreasonable "bullying" in the last years indicate that the software industry in general is still in its infancy and not up to the task of understanding the effect of the increasing role of software systems in managing the modern world, from digital services to critical infrastructure, and taking it seriously.

I promise that all of the future code which will make it into the codebase is fully human-crafted and audited. 
I don't have "productivity" concerns, because I have realized that most of the industry in the last decades was very "productive" at piling up utter garbage!

Feedbacks and suggestions are welcomed, but this project will not have an open code-contribution model.
All the code that gets merged into the codebase will be authored by the project's members and delegated persons **only**, 
hence there is no public policy of disclosure.

I am very happy to know that more and more people are leaving this "bubble" and starting to realize the limitation of this technology, basically seeing it for what it is, at least in this domain, and go back to do "serious" things in the hope that they could be impactful in one way or another.

Btw, I am already a "prompt engineer", I prompt with **semantic"** language using Rust's syntax, and let the marvelous **semantic** agent, the Rust's compiler, generate the machine-code for me! and I hope this marvelous **semantic** agent gets even better.