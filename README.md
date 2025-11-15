# plcbundle-rs

Main purpose of this package is to provide universal and simple interface, as Rust library or C/GO bindings for manipulation with plcbundle repository.

## 🚨 Important for Contributors and AI Assistants

**Please read [`RULES.md`](RULES.md) before contributing or generating code.**

Key principle: **CLI commands and server code NEVER open bundle files directly** - all operations go through the `BundleManager` API.