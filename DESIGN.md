Interconnect's design aims at providing a framework for bridging the interaction between applications, systems and devices, local and remote alike, by constructing services that act as protocols in their own right, without relying on other protocols as underlying carriers. Interconnect is in some sense a **meta-protocol**, a protocol that defines protocols.

> **Note**: The design details apply to the reference implementation in **Rust**. Implementations in other languages may diverge according to the capabilities and limitations of the implementation language.

## Architecture and general design approach

Interconnect has a dataflow-oriented architecture where data availability drives computation.

Interconnect defines the concept of "component" as the basic unit of composition at the implementation level.

Interconnect's design utilizes a **meta-system** that uses `server` as a universal abstraction to describe its model of composition. According to this **meta-system**, the software is a network and the hardware is a network, and all components on all levels are `servers`.

From design perspective, components are **viewed** as **micro-servers** offering **services** that can be static, dynamic, local and remote.

Interconnect doesn't recognize the concept of "data structures" as independent entities in programming, and rejects the definition of programs as "algorithms + data structures".

Data structures are **algorithmic** entities **by necessity**. Understanding data structures as algorithm-agnostic is **major fallacy**, and implies misunderstanding of the concept of algorithm itself, because organizing storage is **part** of implementing a solution (algorithm) to a problem.

Data structures are viewed as **storage schemes** related to particular algorithms, and those algorithms could be a set of other smaller algorithms.
Within the context of this project, the concept of "data structure" – if used – refers strictly to the **representation of data** in memory, or in other words, how the **data is structured** in memory.

We usually refer to common schemes like hashmaps or linked lists as "data structures", but they are **algorithms** that require a **storage scheme**, and **can** be used in "larger" algorithms for the sake of the **reuse**, with whatever accompanied **tradeoffs** regarding strict fitness and efficient integration.
 
Components are encapsulation of algorithms at the implementation level.

Interconnect's design favours variety of optimized components over common ones with complex configurations in general.
Diversity with optimized internals and simple setup is considered a better strategy than uniformity with wide set of configuration options.

Interconnect's design is layered with modular components where each layer has specific responsibilities, allowing flexible composition without sacrificing coherence and efficiency.

These layers are typically combined and orchestrated by role-based components like client and server that specify the requirements in order to make these layers work well together as a single whole.

Interconnect's **main** layers are from upper to lower:

**Service Layer**:
Service layer serves constructing of user-defined messages and processing of received user-defined messages.

Interconnect's services are designed to be stateful when needed with the ability to manage their own sessions.

**Transaction Layer**: 
Transaction layer adds and interprets the control metadata of the passed messages and performs dispatch via the transport components. 

Moreover, it implements the machinery to safely and correctly encode and decode the **defined message** for the **target method**.

By default, messages are passed carrying borrowed data, and get returned after receiving carrying borrowed data.

Thanks to the custom-layout and strict alignment rules, all types in a message can be accessed borrowed without conversion to owned types and with the lifetime-bound as the only restriction applied, something that would be very limited, not possible or recklessly unsafe in the "naïve" common world of encoding and decoding out there.

However, received messages allow conversion to owned types when borrowing can be restrictive.

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

## Data exchange
Interconnect's unit of exchange is "message".

Messages are exchanged in binary format with **untagged** data representation.

The reason for not using tags is that these add unnecessary overhead to storage and transmission, and they complicate the layout of the message, which is designed for zero-copy direct access.

Many people usually refer to **tagged representations** as **"self-describing"**, but there is no such thing as "self-describing" in computing. Nothing is **describable** or **meaningful** in itself without using a proper decoder.

So instead of wasting processing power on decoding tags and constructing intermediate types which **hopefully** can be converted successfully to native types, Interconnect decodes and validates the content of the message, and makes the content usable directly without conversion.

Interconnect defines its own data model that describes the Interconnect's core types and their representation and the layout of messages.

Interoperability is achieved by adhering to the ABI (Application Binary Interface).

Interconnect requires native implementation of its type-system and its associated components like encoders and decoders 
in order to send and receive messages that conform to its data model.

The native implementation of the type-system and other support components like the transport components and the endpoints' 
implementations are referred to as the "runtime library".

The runtime-library provides components to construct a compliant implementation of Interconnect.

The specifications and the details of the data model are described in-depth in [SPECS](SPECS.md) file and updated regularly.

Moreover, Interconnect defines a declarative language which is referred to as "Service Definition Language (SDL)", and provides toolchain like compiler and code-generation backends for generating integrated endpoints that conform to the user-defined interface.

The SDL and its toolchain are an **usability and safety** option for making use of Interconnect in a highly automated manner.

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
- It is very difficult to reliably know the state of the memory after panic (E.g. Many "UnwindSafe" data structures will leak resource).
- Unwinding complicates the design of components (e.g. Panic tracking fields/flags, think std::sync::poison, which will "poison" the entire codebase with checks).

Theoretically, unwinding can be used for implementing a very efficient error handling strategy, by means of reducing error checks in each call-frame and carefully designed "catch-points" in the call chain, but this model is less flexible and very tricky to setup and maintain properly, especially across refactorings. 

On the other hand, forcing abort simplifies the implementation of components, yields a very lean machine code and enables a lot of optimization opportunities to the compiler, which would eliminate a lot of overhead, a strategy by far the more practical and reliable choice to consider.

Interconnect's design differentiates between control-flow errors and error-reporting with two separate types:
- Constrained error-type.
- Reporter instance providing access to the reporting subsystem.

Returned errors serve **informing** the calling context which path to take after an error has been encountered, 
with enough information to serve as **branching flags**.

Error-reporting is performed via a reporting subsystem exposed via a reporting-component, that produces reports with certain structure and format. Depending on the reporting-component used, reporting can add a non-trivial overhead to the system if utilized without careful consideration, therefore reporting is considered a privileged capability that shall be given transparently.

## Memory allocation

Memory management is a **major** performance and stability factor.

The common approach in this regard is to let components allocate memory unconstrained on-demand at multiple places via the global general-purpose allocator, and to deallocate that memory when the work with it ends. This common approach is very flexible but it is inefficient in terms of performance, and can lead to high memory fragmentation.

From design perspective, memory is considered a **service**, provisioned via **memory servers**.

Components ask for memory from an explicitly passed memory server that may provide a block of memory which can be accessed, shared and managed as **protocoled service**. Each component or layer can define its own requirements in terms of protocols required to access that memory.

The core idea is to enable sharing and reusability of the same acquired memory across components and layers as much as possible.

Interconnect's design is **centered** around memory subsystems and their implementation for various cases is an **essential** part of the project.

## Security

Security is **marginal** and subject to the required margin and the cost of performance and complexity.

Security considerations are part of the core design and they are taken into account on all levels.

Interconnect's design defines three categories of security:

- **Transport security**: Securing the communication via encrypted channels that protect **confidentiality** and **integrity**.

  Securing the transport is the responsibility of the transport components and it is subject to each implementation and its defined threat-model.

  The security of transport is **independent** from other security procedures. Static secrets are **prohibited** from being used as keys to secure a transport session.

  Static secrets can be used for identity verification and access control, but **never** to secure a transport session.

  All key-exchange protocols **must** deploy perfect forward secrecy algorithms using **ephemeral** key-materials.
  
- **Identity verification**: Verification process of valid **identity** as proof of trusted origin.

  Identification helps peers to make sure they are communicating with what they are expecting as peer.

  When using secure transport, identity-exchange **must** take place **after** establishing secure connection. 
  Secure transport protocols **must** never implement identity-exchange as part of their establishing phase.

  Verification of valid identities is the responsibility of components that are referred to as identity-providers. 

- **Access control**: Enabling operations according to adequate privileges.

  Access control is the responsibility of the service implementation.

  This can be achieved **without** any special protocol-level support, especially that Interconnect's services are designed
  as **stateful servers** that require explicit termination. 
  
  The defined service-methods are accessible via session's instances returned by the service instance, and they **can** operate in full awareness of the connected peer, if the service-implementation takes advantage of this feature. If the service doesn't need access control, it can return peer-agnostic sessions without any added overhead. That is to say that there is no fixed cost, and
  the runtime cost is always relative to what is being used.
  
  For example, the service can be defined with authentication methods as the following:

  ```rust
  interface ProtectedService {
    // Explicit authentication call.
    // Since services are stateful, the service will keep this stored until logout,
    // or until dropping the connection (variety of policies may apply).
    login(LoginMessage): LoginResult;

    // Explicit logout, although logout can be executed automatically when dropping connections. 
    logout();

    // This call is open to all peers and doesn't require authentication. 
    unprotected_call(): ResultA;

    // This call requires authentication and possibly authorization.
    // Without active login, it returns error to signal the necessity of authentication first.
    // With active login but lack of privileges, it returns also an error.
    protected_call(): ResultB;
  }
  ```

  The above example achieves the goal efficiently without baking special metadata into each message, and without any clunky middleware like interceptors.

## Reference implementation

The reference implementation of Interconnect is in `Rust`.

Rust as a choice was not a random choice or out of hype, this decision has been taken with carful considerations of key points:

- Control over memory layout and memory access.

- Control over the lifecycle (No GC).

- Control over referencing and moving (No language-dictated value-types and reference-types).

- Statically (AOT) compiled code with static typing and high level of optimization (No VM).

While other languages like C/C++ satisfy these requirements, they carry a decades-long baggage and they lack the syntactic constructs to track referencing/aliasing and to constrain mutable referencing, so they fell out of the choice very quickly. Other languages in this league satisfy these requirements but they lack many of Rust's syntactic constraints besides stability.

Rust in many ways not a new language, but its unique position lies in baking all of these stuff in a coherent and an elegant language, with very successful execution uppon ideas.

Programming languages are different obviously, but the differentiation standard has not yet been well established in the industry.

Many programming language has advantages and disadvantages for certain things in particular, but the industry could do itself a great service and save a lot of time and money if it puts languages in 4 categories like the following:

- Experimental languages: PL-theories' playground.

- Research languages: Used mainly in labs for rapid prototyping and testing.

- Domain-specific languages: Used for specific tasks at small scale (think queries or scripting).

- **Engineering** languages: Meant to implement **production** systems.

Without going into the details of specific languages, most common languages with GC and VM have their roots in the second category, in **research**, with **Lisp** and **Smalltalk** as direct inspiration. Because of this I would like to elaborate on what went wrong here:

- VM: In both Lisp and Smalltalk, the VM carried the "the future hardware emulated" ethos where the VM was either a **microcode** loaded into the processor or a **hardware** implementation or semi-implementation of the language itself later. None of these language promoted the idea of "write once run anywhere", where the VM is a software-based "third" platform on top of "abstracted" hardware and OS platforms respectively. Both languages were betting on hardware implementation of their semantics.

  > "One final comment. Hardware is really just software crystallized early. It is there to make program schemes run as efficiently as possible. But far too often the hardware has been presented as a given and it is up to software designers to make it appear reasonable. This has caused low-level techniques and excessive optimization to hold back progress in program design. As Bob Barton used to say: "Systems programmers are high priests of a low cult".<br><br>One way to think about progress in software is that a lot of it has been about finding ways to late-bind, then waging campaigns to convince manufacturers to build the ideas into hardware. Early hardware had wired programs and parameters; random access memory was a scheme to late-bind them. Looping and indexing used to be done by address modification in storage; index registers were a way to late-bind. Over the years software designers have found ways to late-bind the locations of computations—this led to base/bounds registers, segment relocation, page MMUs, migratory processes, and so forth."
  >
  > — Kay, A. C. (1993). *The early history of Smalltalk*. *ACM SIGPLAN Notices, 28*(3), 69–95. https://doi.org/10.1145/154766.155364
  >
  > More "human-readable" version: https://worrydream.com/EarlyHistoryOfSmalltalk/

  More recent view to this approach [here](https://youtu.be/V_Vn5rz6hL0?t=5423).

  So the **virtual** machine is supposed to be (more or less) **materialized** as an actual machine. Running applications and systems on top of such "VM" means adding an **emulation layer** to each instance without reasonable objective justification, at least to me, I don't see the point.
  
  There are flawed narratives floating around for a while to "justify" this "miss-take" (Our minds are cleaver at this, especially if we have some investments there!), one of them is "portability". Now, the "portability" narrative is flawed because "portability" doesn't exist practically. This approach assumes that the target-specific "VM" already installed on the user's machine with the right version, often forcing the user to install the "VM" with the required version in order to make use of the deployment package, leading to a crappy user experience and "VM-bloat" on the target machine. Shipping the "VM" with package contradicts the narrative entirely. In the case of servers, the hypothesized "portability" makes no sense whatsoever, because the target is already known and the entire workflow is automated anyway.

  Another flawed narrative is machine-dependent optimization, which is to say that the "VM" is capable or doing optimizations specific to the machine it runs on. The first problem is that the "VM" does this at the **wrong time**, when the code is being executed. The second problem is that it can't do much because it can't afford complex analysis of the entire codebase (naïve cute patchy optimizations of hot paths ..etc). The third problem is that this mechanism is profile-guided not a special advantage, pretty much the same what ahead-of-time compilers do, but compilers have **major** advantages like operating at the **right time**, the **compile-time**, and doing sophisticated analysis and **verification** before the deployment package ever reaches the user. 
  
  There is an interesting view on the topic from Simon Jones [here](https://youtu.be/xcB_LF3cdqw?t=1003).

  The last point worth mentioning here is that hardware is not **free** or **cheap**, it still cost **time**, **space** and **energy**.
  Bloating the hardware with features that the compilers should take care of in the first place is a waste of space, energy and **precious time**, **run time**. What can be done at compile time **MUST** be done at compile time, leaving the physics for better things to accommodate and handle.

- Garbage collector: Both Lisp and Smalltalk employed GC, but again, both languages were "lab" languages. Lisp was born at an AI-lab and Smalltalk was born at PARC for experimenting with UI and dynamic environments (Smalltalk is a language-environment combination actually without OS and it is the origin of the concept of **IDE**, because the **execution** environment **is** itself a **development** environment with integrated versioning system, that is to say that changes are reflected in the running system, live. [More details from the creators](https://www.youtube.com/watch?v=PaOMiNku1_M)). 

  Even in that context, the attitude was similar to VM in sense that, if GC is needed, it shall be a implemented in hardware.

  Objectively speaking, GC is **bug** not a feature, it is a crude and primitive approach to automatic cleanup, and I don't know why it has been **romanticized** instead of being **criticized** all these years in the industrial and academic circles and without active effort to make better mechanisms given that, even before Rust, **Ada** (A serious **engineering language**, from serious designers!) had automatic cleanup **without** GC **decades** ago.

- Object-Oriented Programming: Writing all of this, I presume I am already in middle of the hornets' nest, so some elaboration here would not make it worse! This topic is a little bit complicated from historical perspective, but the term has been coined by **Dr. Alan Kay** inspired by Sketchpad, Simula and the biological cells on our bodies in the late 1960s to describe a new programming model based on "objects" as units of composition.

  These entities that are referred to as "objects" are supposed to:
    - hide their states (Encapsulation).
    - hide their implementation (Polymorphism).
    - adopt properties and "behavior" from other objects (Inheritance).
    - invoke "behavior" on other objects via "sending messages" (Dynamic dispatch).
    - operate in dynamic environment (Late-binding)

  Later definition according to Dr. Kay came to be:
  > "OOP to me means only messaging, local retention and protection and hiding of state-process, and extreme late-binding of all things. It can be done in Smalltalk and in LISP. There are possibly other systems in which this is possible, but I'm not aware of them."
  
  More details about the original concept [here](https://www.youtube.com/watch?v=QjJaFG63Hlo&t=464s).

  Now, common languages known as object-oriented (e.g C++, Java, C#) **allegedly** have diverged from this definition and presented a static model that did not take the "messaging" part into account. This "messaging" part remains an opaque description though, because in all implementations it is a **plain procedure call**, regardless of how the right procedure to invoke is determined. Moreover, this "messaging" part is required to be **non-commanding** in some sense, which is also an opaque requirement without specific meaning. So I am not sure how the "messaging" part has been ignored, and in what sense.

  For the sake of making this short, one of the most common feature of the OOP model is **inheritance**, which establishes a hierarchy of "objects" that share data (state) and behavior (methods). 
  
  **Inheritance** as a **prominent** feature of the OOP as known in the wild is poisonous to me, it is a single worst idea ever introduced to the programming world, it complicates the implementation, adds ambiguity, shares states with a possibly very large chain with very strict regime of "initialization".

  Since each language that "claims" to be object-oriented implements it differently, it is hard to speak about efficiency in general, but in the case of e.g. Java/C# it is inefficient and adds VTable-lookup overhead for no objective reason. Python, Ruby & Co do hash-lookup at runtime with method resolution order (MRO) (classes basically hashmaps).

  Both **dynamism** and **late-binding** are useful **options**, but I don't see why they must be a universally enforced language-level regime. From engineering perspective, there is no objective reason to **force** dynamism on types uniformly without giving the user a choice. "Polymorphism" is better served by parameterizing with static substitution for both safety and performance (energy). Rust's **trait-system** solves the problem with very clean and efficient way, leaving choosing the dynamic aspects to the user where required.

  **Programming** is **communication** with dual duty, communicating the intent to the machine and to other humans reading the code. A sane modern programming language shall not conform to **any** paradigm, it should incorporates ideas that help **fulfilling** its duty as an efficient communicator with the hardware, and as a clear and detailed descriptor of the intent.
  
The point of mentioning this little piece of a very ancient history is that, on one hand tinkering and research lean toward flexibility and ignore control and efficiency, on the other hand the modern heterogenous hardware relies on very sophisticated compilation techniques with a lot of analysis and transformations on multiple levels to deliver performance.

I would say that most common languages with GC and "VM" didn't offer tradeoffs, they simply missed the historical context and the design-assumptions and preconditions, and have been pushed by aggressive marketing (think Sun and its Java), especially to places where they supposed to be very **critical** of this "deceptive" simplicity like universities and engineering institutes.

Bringing languages of this category to the engineering space means scalability-limits of what future improvement to that codebase can be done besides hardware-related costs and above all **energy**. There is no better example than the current situation with ML where research relies on python, but taking python into the engineering space would turn the planet earth into "inferno" burning resources at all levels.

Maybe a better way to look at this with probably the right perspective is that a lot of this language - and tools business has to do with an emerged conflict between two groups, the domain experts and the "computer" people.

IT and "computer people" are viewed usually by many traditional organizations (almost the majority) as "necessary evil", with "computer people" being viewed as operating in their own world, on the other hand computer people usually view domain experts as complicated archaic bureaucrats with weird rules. Depending on the organization, these views could be milder or even more extreme.

Over the years, this gave birth to three main patterns that **repeat** in waves every once in a while. The first pattern is to dump down programming by providing very "high level" languages as an attempt to make the "end-user" developer (e.g. OOP, "Low-Code", visual programming with blocks, scripting DSLs, the current lousy attempt with "programming in plain english" fad), then marketing people jump in to preach the new "era" of making software. The other pattern is to expect developers to have deep domain expertise. The third pattern is to have intermediaries like having people translating between both groups in some way (UML, BPMN, ..etc).

Now, making software-systems (an App is a system also!) is not about modeling "domain", it is fundamentally an act of **engineering** a **machine** that serves processes which require sophisticated facilities that enables higher level of **control** and the construction of **custom components** which are able to take full advantage of the underling hardware as the lowest platform in the hierarchy, with maximum efficiency as a response to the scale and serving around the clock and overall cost of that (Money is a poor and **temporal** representation of both cost and value!).

There are arguments floating around for a while for "very high-level" programming models as an answer to safety and complexity. The problem is that – practically – the higher a language goes, the more "specialized" it becomes. Practically, there is **no way** to do "very high-level" programming model without ending up limiting its coverage, all while requiring an "escape hatch" to regain control and implement/reimplement things anyway. 

The **closest** examples of such "very high-level" general purpose programming schemes are functional programming and the **so-called** logic programming. Functional programming in its **strict definition** abstracts control flow by higher-order functions and recursion, and prevents mutating state, which makes it both, inefficient and useless in its **strict form**. That is not to say that the landscape of functional programming is uninteresting, on the contrary, actually it is very interesting as a research area, especially around the type systems among other things, but none of these interesting stuff are inherent to the model. Logic programming on the other hand views computation as "controlled deduction", and it has its root in relational databases and knowledge bases. Logic programming models programming as defining predicates that are evaluated as queries/goals via **inference engines**. The last attempt to make logic programming a mainstream programming model was the so-called ["Fifth Generation Computer Systems"](https://en.wikipedia.org/wiki/Fifth_Generation_Computer_Systems), which was a failure. There is an interesting paper on its failure [here](https://www.sciencedirect.com/science/article/pii/S0167642317301235?fr=RR-2&ref=pdf_download&rr=a0bb9b983a3c362f).

Without going deep into the details of both examples, the common scheme in both is abstracting the control imperatives behind higher-level declarative expressions at the language-level, which gives the programming system **total control** over the mapping to lower-level control schemes, and eventually to machine code, with basic assumption that the system is **able** to map the declarative expressions to **suitable** lower-level control schemes for all domains, and such assumption is historically proven to be wrong.

The beauty of the "imperative" code as we know it is that it is a sort of the "middle ground" between declarative semantic schemas and the actual machine code, and this gives it an immense power in expressing higher level logic of different domains with very high level of flexibility regarding the control aspect (we let compilers generate the machine code after all). So in practical terms, higher "levelness" is better served with components and tools provided by libraries and frameworks for encapsulating the "control" details and aiding development using a unified "hierarchal" programming model based on fine-grained control as its core **accessible** model, which can be encapsulated in "higher" level components that represent the higher level semantics (macros can be helpful..sometimes!).

CAD-Sim-Fab workflow works in other domains of engineering because it is **application-specific** with very **limited** set of options. Software systems **don't** have constraints, not in that sense at all, on the contrary, they **define** constraints as an **implementation** of the modeled process. Making/having set of tools for aiding the design and testing/verifying of a particular application could be very helpful, but this is again **application-specific** and **can't** be at the language's level, not without ending up with DSL. Such tools already in use for compiling the software of "control" systems, and with full HIL (hardware-in-the-loop) simulation. In any case, if the modeling is wrong, the final software would be perfectly **correct** doing the **wrong** things!

In short, the "mess" we are in is by and large because we have't figured out a proper organization for making use of computing and constructing computing systems which **require** the involvement and the collaboration of different expertise with different perspectives. Computing is still an ad-hoc, afterthought, patchwork-driven part of modern organizations who are very fragile and vulnerable to "rosy" claims.

Computers are everywhere, but not computing! Organizations have not yet organized themselves properly around "computing". Computers are still being wired as add-hoc convenient "paper-simulators" instead of being an agent with proper modeling of processes as computational processes, with established healthy culture of collaboration and integrating expertise, instead of the wild-west of ad-hoc tools and standards where everybody does his own thing alienated and alienating others.

Choosing a programming language is a **strategic** decision, because it will set the limits on what can be done next and what costs are ahead when the project evolves. I hope the industry could learn the lessons of the last decades and choose an **engineering** language for its **engineering** project **always**, not as a last resort because nothing else works.

Rust without doubts is an **engineering** language and more, because it has got a gentle "safe" surface with automatic memory management that helps with getting things done rapidly and enables developers to **define** "some" of their **constraints**, but more importantly, the fine-grained control remains an option at any point of the project, and that is why rust has been chosen.