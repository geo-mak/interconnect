# Interconnect Service Definition Language Specification (Draft)

This document defines the specification of interconnect's data model and how it is expressed in its language.

The following details have implementation version, but its current purpose is to aid design.

> **⚠️ Specifications are subject to change without prior notice.**

## 1. Primitives

| Type       | Description                     |
|------------|---------------------------------|
| `bool`     | Unsigned 8-bit integer with two values: <br> - `0x00`: `false`  <br> - `0x01`: `true` |
| `u8`       | Unsigned 8-bit integer          |
| `i8`       | Signed 8-bit integer            |
| `u16`      | Unsigned 16-bit integer         |
| `i16`      | Signed 16-bit integer           |
| `u32`      | Unsigned 32-bit integer         |
| `i32`      | Signed 32-bit integer           |
| `u64`      | Unsigned 64-bit integer         |
| `i64`      | Signed 64-bit integer           |
| `f32`      | 32-bit IEEE 754 floating point  |
| `f64`      | 64-bit IEEE 754 floating point  |

Primitive types have **little-endian** representation regardless of the architecture.

## 2. Collections

Collections serve grouping multiple elements of the **same** type.

### Vector

**Description**: Variable-length sized group of elements.

**Representation**: The length and sequence of values of the storage type.

**Storage**:
  - Inline: Metadata consists of the length and a pointer to elements' data.
  - Out-of-line: Sequence of values of the storage type.

**Syntax**: `[T]`, where `T` is the storage type. 

### Array

**Description**: A fixed-length group of elements.

**Representation**: Sequence of values of the storage type.

**Storage**: Inlined.

**Syntax**: `[T; N]`, where `T` is the storage type and `N` is the length. 

### UTF-8 Vector

**Description**: Variable-length sequence of UTF-8-encoded bytes.

**Representation**: The length and a sequence of UTF-8-encoded bytes.

**Storage**:
  - Inline: Metadata consists of the length and a pointer to elements' data.
  - Out-of-line: Sequence of UTF-8-encoded bytes.

**Syntax**: `string`. 

### UTF-8 Array

**Description**: Fixed-length sequence of UTF-8-encoded bytes.

**Representation**: Sequence of UTF-8-encoded bytes.

**Storage**: Inlined.

**Syntax**: `string[N]` where `N` is the length. 

## 3. Compound Types

Compound types aggregate multiple fields or variants.

### Enums

**Description**: A group of named constants.

**Representation**: Integer-value with representation depends on the specified integer-type (e.g. `u8`).

**Storage**: Inlined.

**Syntax**:

```rust 
enum EnumIdent: type { 
  VariantIdent = 0,
  ..
}
```

**Constraints**: 
  - Enums can't be empty.
  - Tag's value is explicit.

**Modification**: Modifying the tags of the variants is a breaking change.

**Example**:

```rust
enum EnumA: u8 {
  A = 0,
  B = 4,
  C = 2,
  D = 3,
}
```

### Union

**Description**: A tagged union with multiple members.

**Representation**: A Tag, size and the data of the active member.

**Storage**:
  - Inline: Metadata consists of the tag, size and a pointer to member's data.
  - Out-of-line: Active member's data.

**Syntax**:

```rust 
union UnionIdent {
  1: MemberIdent: type,
  ..
}
```

**Constraints**: 
  - Unions can't be empty.
  - Tags can't be negative.
  - Tags can't be sparse.

**Modification**: Modifying members or their tags is a breaking change.

**Example**:

```rust
// 16-bytes inline size aligned to 8 bytes.
union UnionA {
  1: MemberA: StructA,
  2: MemberB: EnumA,
  3: MemberC: f32,
}
```

### Struct

**Description**: A fixed-layout collection of named fields.

**Representation**: Array of bytes with layout aligned to the alignment of the largest scalar member.

**Storage**: Inlined.

**Syntax**:

```rust
struct StructIdent { 
  ident: type, 
  ..
}
```

**Constraints**: Structs can't be empty.

**Modification**: Modifying fields is a breaking change.

Interconnect's structs are identical to C-structs in terms of memory-layout.

**Example**:

```rust
// Struct's size: 24 bytes.
// Struct's alignment: 8 bytes.
struct StructA {
  // Size: 16,  Alignment: 8,  Offset: 0, Padding: 0.
  first: [StructB],
  // Size: 4,  Alignment: 4,  Offset: 16, Padding: 0.
  second: f32,
  // Size: 4,  Alignment: 4,  Offset: 20, Padding: 0.
  third: f32,
}
```

Struct's fields allow assignment of default values, but like optionality-rules their rules are still not well-defined.

```rust
struct StructB {
  first: i32 = -5,
  second: f64 = 5.6,
  third: string = "hi there",
}
```

### Message

**Description**: User-defined transactional unit of data exchanged between the two sides of the interface-boundary.

**Representation**: Represented as `struct` with identical layout-rules.

**Storage**: Inlined.

**Syntax**:

```rust
message MessageIdent { 
  ident: type, 
  ..
}
```

**Constraints**:
  - Messages can't be empty.
  - Messages can't be fields of anything, including other messages.
  - Messages are the only types that can cross the API-boundary, all other types are fragments of their data.

**Modification**: Modifying fields is a breaking change.

User-defined messages are sent and received with additional **control** metadata. 

The layout of user-defined messages consists of two main regions:
- Inlined region: Stores the fields of the message as defined, where each field represents inlined-data or inlined-metadata.
- Out-of-line region (if applicable): Stores the sequence of the data blocks referenced by the fields that store metadata.

The out-of-line data blocks are appended after the inline-layout in **traversal order**.

Both regions are aligned to **8-bytes**, this implies that the allocated encoding/decoding memory must be aligned to 8-bytes.

## 4. Interfaces

**Description**: A collection of IPC functions.

**Syntax**:

```rust
interface InterfaceIdent {
  // Niladic one-way call.
  ident();

  // Niladic two-way call.
  ident(): MessageDef;
          
  // Monadic one-way call.
  ident(param: MessageDef);

  // Monadic two-way call.
  ident(param: MessageDef): MessageDef;
}
```

**Constraints**: Interfaces defines functions that can take `message` types as arguments and return `message` types **only**.

**Modification**:
 - Modifying an existing function is breaking change.
 - New functions can be **added**.
 - Existing functions can be **deprecated**.

Interfaces guarantee the semantics of the IPC in terms of sent and received messages, but their concrete implementation
can vary. The exact implementation depends on runtime-libraries used for the implementation.
For example, a generated function may return a union of the defined message and an error-type specific to the runtime.

Moreover, there is no dedicated syntax for annotating `async`, because it is considered an implementation detail and it is 
not part of the exchange-semantics between the two sides of an IPC-boundary.

The IPC-definition is concerned mainly with the data-model and its correct expression in terms of exchange layouts, regardless 
of the runtime-config.

Interfaces are the **only** means of managing the evolution of the service. 

In order to change/update a message or any of its members, a new message is required with a **new function** that takes that message as parameter or returns that message as a response. The old message and its function must be kept **unchanged** and the old function shall be annotated as `deprecated`. This will prevent the deprecated function from being accessible by the client in newer systems, while allowing the service provider to handle older systems gracefully.

Dynamic messages and other types that allow non-breaking modifications like deprecation and addition of fields have been implemented and removed, because they add complexity and they have mediocre performance. Basically, all fields in such setup have to be made optional, where each access to a field has to be a checked access (think tables of Flatbuffers and FIDL).

Centralizing change-management around interfaces is a simpler and more performant approach, because there is a typical fixed cost of matching calls anyway, where calls can get special handling if their parameters or return types have been changed.

The behavior in the deprecation case on client-side is simply to never generate the function or any of its dependencies.
On the server-side the behavior is not yet fixed, but there are two options floating around:
- No generation of dependencies and no processing of the call, just returning an error on arrival.
- Maintaining full support of older clients before making the call an error later. 

One thing is settled, no dynamic or tables-like types will be used for the service-evolution ever.

Depending on the transport-model, the defined functions are typically identified using `u64` value derived from passing the function's
signature to a special hash-function at compile-time.

For better reliability and explicitness, explicit tagging **might** get added to the syntax, where the each function would be identified by its specified tag (like unions) instead of hashing its signature. This would make manual implementation also easier.

## 5. Attributes

**Description**: Compiler directives that add extra context to the defined element.

**Syntax**: 
  - Zero-modifier attribute: `@attribute`.
  - Single-modifier attribute: `@attribute(modifier)`.
  - Multi-modifier attribute: `@attribute(modifier, modifier, ...)`.

**Constraints**: Each definition accepts specific set of attributes only.

**Example**:

```rust
interface ServiceA {
  // Annotate the call as deprecated while maintaining full support for older versions.
  @deprecated
  call_a(): MessageDef;

  // Annotate the call as deprecated with error returned when called.
  @deprecated(error)
  call_b(param: MessageDef): MessageDef;
}
```

## 6. Namespaces

**Description**: A grouping namespace for the generated code.

**Syntax**: `namespace name`.

**Representation**: Target-specific. `mod` in Rust.

All of the generated code from a source file will be accessible under the defined namespace.