# Elements and coding

An element is a value carried by a `PCollection<T>`. Ironbeam's `Element` contract supplies a stable type identity and, where applicable, a coder identity. Primitive types already implement the contract. User records can implement `Element` directly or use the opt-in Postcard helper.

```rust
use ironbeam::Element;

#[derive(Clone, Debug)]
struct Record { value: i64 }

impl Element for Record {
    const TYPE_ID: &'static str = "example/record/v1";
    const CODER_ID: &'static str = "example/record/local-v1";
}
```

The direct implementation above is suitable for local processing when no portable encoding is required. A portable worker must know the same element type and coder. With the root `coders` feature, Serde records can use `impl_postcard_element!(Record, "example/record/v1")`. Change the coder ID when the wire representation changes; retain stable IDs for compatible restarts.

Event time and window assignment are metadata on values, rather than fields automatically inferred from a record. See [windowing](../guides/windowing.md).
