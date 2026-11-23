//! TypeScript generation for the registered sources (feature `typegen`).
//!
//! With the feature on, every value and argument type passed to a registration method must
//! implement [`ts_rs::TS`] (`#[derive(TS)]`). [`Reactive::export_typescript`] then writes one
//! self-contained file with every referenced type plus a `Sources` map for `createReactive<Sources>()`.

/// Bound on registered value/argument types: `ts_rs::TS` with the `typegen` feature, nothing without.
#[cfg(feature = "typegen")]
pub trait MaybeTs: ts_rs::TS + 'static {}
#[cfg(feature = "typegen")]
impl<T: ts_rs::TS + 'static + ?Sized> MaybeTs for T {}

/// Bound on registered value/argument types: `ts_rs::TS` with the `typegen` feature, nothing without.
#[cfg(not(feature = "typegen"))]
pub trait MaybeTs {}
#[cfg(not(feature = "typegen"))]
impl<T: ?Sized> MaybeTs for T {}

#[cfg(feature = "typegen")]
pub(crate) use imp::*;

#[cfg(feature = "typegen")]
mod imp {
    use std::{any::TypeId, collections::HashSet, fmt::Write};

    use ts_rs::{Config, TypeVisitor, TS};

    /// What a source looks like in TypeScript, captured at registration time.
    #[derive(Clone, Copy)]
    pub(crate) enum TsSource {
        State { writable: bool, value: TypeFn },
        Stream { value: TypeFn, args: TypeFn },
        Sink { value: TypeFn, args: TypeFn },
    }

    /// Returns the TS reference for a type, registering its declarations with the collector.
    pub(crate) type TypeFn = fn(&mut Collector) -> String;

    pub(crate) fn type_ref<T: TS + 'static + ?Sized>(c: &mut Collector) -> String {
        c.visit::<T>();
        T::name(&c.cfg)
    }

    /// Like [`type_ref`], but `()` (no arguments) becomes `void` so the JS call takes no args.
    pub(crate) fn args_ref<T: TS + 'static + ?Sized>(c: &mut Collector) -> String {
        if TypeId::of::<T>() == TypeId::of::<()>() {
            "void".into()
        } else {
            type_ref::<T>(c)
        }
    }

    pub(crate) struct Collector {
        cfg: Config,
        seen: HashSet<TypeId>,
        /// (identifier, declaration)
        decls: Vec<(String, String)>,
    }

    impl TypeVisitor for Collector {
        fn visit<T: TS + 'static + ?Sized>(&mut self) {
            if !self.seen.insert(TypeId::of::<T>()) {
                return;
            }
            // Types with an output path are named declarations; everything else is inlined.
            if T::output_path().is_some() {
                let mut decl = String::new();
                if let Some(docs) = T::docs() {
                    decl.push_str(&docs);
                }
                write!(decl, "export {}", T::decl(&self.cfg)).unwrap();
                self.decls.push((T::ident(&self.cfg), decl));
            }
            T::visit_dependencies(self);
            T::visit_generics(self);
        }
    }

    /// Renders the bindings file. `sources` must be sorted by name for stable output.
    pub(crate) fn render(sources: &[(String, TsSource)]) -> Result<String, String> {
        // serde_json writes u64/i64 as JSON numbers, which JS parses as `number`, not `bigint`.
        let mut c = Collector { cfg: Config::new().with_large_int("number"), seen: HashSet::new(), decls: Vec::new() };

        let mut entries = String::new();
        let mut kinds = std::collections::BTreeSet::new();
        for (name, source) in sources {
            let ty = match *source {
                TsSource::State { writable: false, value } => format!("State<{}>", value(&mut c)),
                TsSource::State { writable: true, value } => format!("WritableState<{}>", value(&mut c)),
                TsSource::Stream { value, args } | TsSource::Sink { value, args } => {
                    let kind = if matches!(source, TsSource::Sink { .. }) { "Sink" } else { "Stream" };
                    let value = value(&mut c);
                    match args(&mut c).as_str() {
                        "void" => format!("{kind}<{value}>"),
                        args => format!("{kind}<{value}, {args}>"),
                    }
                }
            };
            kinds.insert(ty.split('<').next().unwrap().to_owned());
            writeln!(entries, "  {}: {ty};", serde_json::to_string(name).unwrap()).unwrap();
        }

        // Distinct Rust types can produce the same declaration: one generic type used with several
        // type arguments (`Wrapper<u8>`, `Wrapper<String>` both declare `Wrapper<T>`), or ts-rs's
        // stand-in types (`serde_json::Value` and its internal `JsonValue`). Keep one copy; only a
        // name with two *different* declarations is a real conflict.
        c.decls.sort();
        c.decls.dedup();
        for pair in c.decls.windows(2) {
            if pair[0].0 == pair[1].0 {
                return Err(format!(
                    "two different Rust types are both named `{}` in TypeScript; rename one with #[ts(rename = \"...\")]",
                    pair[0].0
                ));
            }
        }
        // These names are taken in the generated file by the imports and the `Sources` map.
        const RESERVED: [&str; 5] = ["Sink", "Sources", "State", "Stream", "WritableState"];
        if let Some((ident, _)) = c.decls.iter().find(|(ident, _)| RESERVED.contains(&ident.as_str())) {
            return Err(format!(
                "a Rust type is named `{ident}` in TypeScript, which the generated bindings already use; \
                 rename it with #[ts(rename = \"...\")]"
            ));
        }

        let mut out = String::from(
            "// Generated by tauri-plugin-reactive from the sources registered in Rust. Do not edit.\n\
             // Regenerate with `Reactive::export_typescript`.\n",
        );
        if !kinds.is_empty() {
            let kinds: Vec<_> = kinds.into_iter().collect();
            write!(out, "\nimport type {{ {} }} from 'tauri-plugin-reactive-api';\n", kinds.join(", ")).unwrap();
        }
        for (_, decl) in &c.decls {
            write!(out, "\n{decl}\n").unwrap();
        }
        write!(out, "\nexport type Sources = {{\n{entries}}};\n").unwrap();
        Ok(out)
    }
}
