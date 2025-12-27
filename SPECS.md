# Interconnect IPC Definition Language Specification (Draft)

This document defines the specification of interconnect's data model and how it is expressed in its language.

The following details have implementation version, but its current purpose is to aid design.

**All information in this document are subject to change without prior notice.**

## 1. Primitives

| Type       | Description                     |
|------------|---------------------------------|
| `bool`     | Boolean value (`true`, `false`) |
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

- **Definition**: Variable-length sized group of elements.
- **Representation**: The length and sequence of values of the storage type.
- **Storage**:
  - Inline: Metadata consists of the length and a pointer to elements' data.
  - Out-of-line: Sequence of values of the storage type.
- **Syntax**: `[T]`, where `T` is the storage type. 

### Array

- **Definition**: A fixed-length group of elements.
- **Representation**: Sequence of values of the storage type.
- **Storage**: Inlined.
- **Syntax**: `[T; N]`, where `T` is the storage type and `N` is the length. 

### UTF-8 Vector

- **Definition**: Variable-length sequence of UTF-8-encoded bytes.
- **Representation**: The length and a sequence of UTF-8-encoded bytes.
- **Storage**:
  - Inline: Metadata consists of the length and a pointer to elements' data.
  - Out-of-line: Sequence of UTF-8-encoded bytes.
- **Syntax**: `string`. 

### UTF-8 Array

- **Definition**: Fixed-length sequence of UTF-8-encoded bytes.
- **Representation**: Sequence of UTF-8-encoded bytes.
- **Storage**: Inlined.
- **Syntax**: `string[N]` where `N` is the length. 

## 3. Composite Types

Composite types aggregate multiple fields or variants.

### Enums

- **Definition**: A group of named constants.
- **Representation**: Integer-value with representation depends on the number of variants.
- **Storage**: Inlined.
- **Syntax**:
  IIDL syntax:
  ```rust 
    // Represented as integer-value determined by the compiler.
    enum { 
      Ident1,
      Ident2,
      ..
    }
  ```
- **Constraints**: Enums can't be empty.

### Union

- **Definition**: A tagged union with multiple members.
- **Representation**: A Tag, size and the data of the active member.
- **Storage**:
  - Inline: Metadata consists of the tag, size and a pointer to member's data.
  - Out-of-line: Active member's data.
- **Syntax**:
  IIDL syntax:
  ```rust 
    // Tag is represented as unsigned integer-value determined by the compiler.
    union TaggedUnion {
      1: IdentA: type,
      2: IdentB: type,
      ...
    }
  ```
- **Constraints**: 
  - Unions can't be empty.
  - Tags can't be negative.

### Struct

- **Definition**: A fixed-layout collection of named fields.
- **Representation**: Array of bytes with layout aligned to the alignment of the largest scalar member.
- **Storage**: Inlined.
- **Syntax**: 
  IIDL syntax:
  ```rust
    struct { 
      ident: type, 
      ..
    }
  ```

### Message

- **Definition**: A struct serves as a unit of data exchange.
- **Representation**:
  - Fixed message: Represented as `struct`.
  - Dynamic message: Represented as `VStruct`. `VStruct` is an internal type and can't be defined in the syntax.
- **Storage**:
  - Fixed message: The same as `struct`.
  - Dynamic message: Stored as two consecutive blocks, the virtual layout and the fields' layout.
    The virtual layout has metadata region encodes the total size and the size of the field.
    Additionally, it encodes the `virtual offsets`, which are used to access the fields.
    Fields are only accessed using `virtual offsets`, which may be valid or invalid.
    Accessing invalid offset is safe and indicates the absence of the field in the fields' layout.
- **Syntax**:
  - Fixed messages: 
    IIDL syntax:
    ```rust
      message { 
        ident: type, 
        ..
      }
    ```

  - Dynamic messages:
    IIDL syntax:
    ```rust
      dynamic message { 
        ident: type, 
        ..
      }
    ```

- **Example**:
  IIDL syntax:
  ```rust
        // Fixed message compiled as plain struct.
        message AddParams {
            lhs: i32,
            rhs: i32,
        }

        // Dynamic message compiled as `VStruct`.
        dynamic message DynAddParams {
            lhs: i32,
            rhs: i32,
            @deprecated
            legacy_field: bool,  // May be absent in newer messages, but safe to access in older systems.
            new_field: [u8],  // New field will be observed only by newer systems.
        }
  ```
- **Constraints**:
  - Messages are the only types that know how to encode and decode full exchange layouts.
  - Messages are the only types that can cross the API-boundary, all other types are fragments of their data.
  - Messages can't be fields of anything, including other messages.
  - Fixed messages are ABI-stable only if their layout remains **unchanged**.
  - Refactoring dynamic messages requires adding new fields at the **end**, and annotating old fields as `@deprecated`.

Dynamic messages shall be the preferred choice for maintaining ABI-stability at the service-level.

Messages are sent with runtime-metadata that is not expressible in the syntax (runtime-implementation details).

The runtime-metadata include the header, in addition to transport-specific metadata.

Messages are the only encoding/decoding intermediaries between two sides of the IPC-boundary, where reading from and writing to 
the transport model require a message instance, which exposes APIs for writing and reading their fields by the rest of the application.

## 4. Interface

- **Definition**: A collection of IPC functions.
- **Syntax**:
  IIDL syntax:
  ```rust
      interface IPCInterface {
          // Niladic one-way call.
          @attr
          ident();

          // Niladic two-way call.
          // `: MessageDef` is equivalent to `-> MessageDef`.
          @attr
          ident(): MessageDef;
          
          // Monadic one-way call.
          @attr
          ident(param: MessageDef);

          // Monadic two-way call.
          @attr
          ident(param: MessageDef): MessageDef;
      }
  ```
- **Constraints**: Interfaces defines functions that can take `message` types as arguments and returns `message` types **only**.

Interfaces guarantee the semantics of the IPC in terms of sent and received messages, but their concrete implementation
can vary. The exact implementation depends on the `linked` runtime-libraries used by the code generator.
For example, a generated function may return a union of the defined message and an error-type specific to the runtime.

Moreover, there is no dedicated syntax for annotating `async`, because it is considered an implementation detail and it is 
not part of the "exchange" semantics between the two sides of an IPC-boundary.

The IPC-definition is concerned mainly with the data-model and its correct expression in terms of exchange layouts, regardless 
of the runtime-config.

## 5. Attributes

- **Definition**: Compiler directives that add extra context to the defined element.
- **Syntax**: `@attr`, `@attr(value)`.
- **Constraints**: Each definition accepts specific set of attributes only.

## 6. Name Spaces

- **Definition**: A grouping namespace for the generated code.
- **Syntax**: `namespace name`.
- **Representation**: Target-specific. `mod` in Rust.

All of the generated code from the `iidl` file will be accessible under the defined namespace.