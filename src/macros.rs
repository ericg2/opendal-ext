#[macro_export]
macro_rules! opendal_add {
    (
        $(
            $variant:ident => $config:path : $feature:literal
        ),+ $(,)?
    ) => {
        paste::paste! {
            $(
                #[cfg(feature = $feature)]
                pub use $config;
            )*

            // Serde derive intentionally omitted — custom impls below handle the
            // flat-key format where Dynamic uses `backend` as the key directly,
            // identical to how typed variants emit their kebab-case name.
            #[derive(
                Debug,
                Clone,
                Eq,
                PartialEq,
                strum::AsRefStr,
            )]
            #[non_exhaustive]
            /// Configuration scheme for a repository backend.
            ///
            /// This enum provides strongly typed configuration for known backends,
            /// while still allowing unknown or externally defined backends to be
            /// represented through [`Scheme::Dynamic`].
            ///
            /// This enum is marked as `non_exhaustive`, so consumers should include
            /// a wildcard arm (`_`) when matching against it.
            pub enum Scheme {
                /// A dynamic scheme that is unknown.
                ///
                /// This variant can be used for backends that are not known at
                /// compile time. The backend name identifies the implementation,
                /// and the configuration map contains backend-specific key/value
                /// settings.
                Dynamic {
                    /// The backend identifier.
                    backend: String,

                    /// Backend-specific configuration values.
                    config: std::collections::HashMap<String, String>,
                },
                $(
                    #[cfg(feature = $feature)]
                    #[doc = concat!(
                        "Configuration for the `",
                        stringify!($variant),
                        "` backend.\n\nSee [`",
                        stringify!($config),
                        "`] for the available options."
                    )]
                    $variant($config),
                )*
            }

            impl Hash for Scheme {
                fn hash<H: Hasher>(&self, state: &mut H) {
                    let s: &str = self.as_ref();
                    s.hash(state);
                }
            }

            impl Default for Scheme {
                fn default() -> Self {
                    Self::Dynamic {
                        backend: String::new(),
                        config: HashMap::new()
                    }
                }
            }

            impl Scheme {
                pub(crate) fn dynamic<T, K, V>(
                    scheme: impl AsRef<str>,
                    value: T,
                ) -> Self
                where
                    T: IntoIterator<Item = (K, V)>,
                    K: Into<String>,
                    V: Into<String>,
                {
                    Self::Dynamic {
                        backend: scheme.as_ref().to_string(),
                        config: value
                            .into_iter()
                            .map(|(k, v)| (k.into(), v.into()))
                            .collect(),
                    }
                }

                #[doc = "Attempts to create an Operator based on the Scheme."]
                pub fn operator(&self) -> opendal::Result<opendal::Operator> {
                    match self {
                        Scheme::Dynamic { backend, config } => {
                            opendal::Operator::via_iter(
                                backend.clone(),
                                config.clone(),
                            )
                        }
                        $(
                            #[cfg(feature = $feature)]
                            Scheme::$variant(cfg) => {
                                Ok(
                                    opendal::Operator::from_config(cfg.clone())?
                                        .finish()
                                )
                            }
                        )*
                    }
                }
            }

            $(
                #[cfg(feature = $feature)]
                impl From<$config> for Scheme {
                    fn from(value: $config) -> Self {
                        Self::$variant(value)
                    }
                }

                #[cfg(feature = $feature)]
                impl From<&$config> for Scheme
                where
                    $config: Clone,
                {
                    fn from(value: &$config) -> Self {
                        Self::$variant(value.clone())
                    }
                }
            )*

            // Serialize as {"scheme": "<key>", "config": <cfg>} — two explicit flat fields.
            // Works identically for typed variants and Dynamic.
            impl serde::Serialize for Scheme {
                fn serialize<S: serde::Serializer>(
                    &self,
                    serializer: S,
                ) -> Result<S::Ok, S::Error> {
                    use serde::ser::SerializeMap;

                    fn to_key(s: &str) -> String {
                        use heck::ToKebabCase;
                        s.to_kebab_case()
                    }

                    let mut map = serializer.serialize_map(Some(2))?;
                    match self {
                        Scheme::Dynamic { backend, config } => {
                            map.serialize_entry("scheme", backend.as_str())?;
                            map.serialize_entry("config", config)?;
                        }
                        $(
                            #[cfg(feature = $feature)]
                            Scheme::$variant(cfg) => {
                                map.serialize_entry("scheme", &to_key(stringify!($variant)))?;
                                map.serialize_entry("config", cfg)?;
                            }
                        )*
                    }
                    map.end()
                }
            }

            // Deserialize from {"scheme": "<key>", "config": <cfg>}.
            // `config` is buffered as serde_value::Value so the buffer is
            // format-agnostic — no JSON roundtrip, works with TOML, YAML, etc.
            // `scheme` and `config` keys may arrive in any order.
            impl<'de> serde::Deserialize<'de> for Scheme {
                fn deserialize<D: serde::Deserializer<'de>>(
                    deserializer: D,
                ) -> Result<Self, D::Error> {
                    fn to_key(s: &str) -> String {
                        use heck::ToKebabCase;
                        s.to_kebab_case()
                    }

                    struct SchemeVisitor;

                    impl<'de> serde::de::Visitor<'de> for SchemeVisitor {
                        type Value = Scheme;

                        fn expecting(
                            &self,
                            f: &mut std::fmt::Formatter<'_>,
                        ) -> std::fmt::Result {
                            write!(f, "a map with 'scheme' and 'config' keys")
                        }

                        fn visit_map<A: serde::de::MapAccess<'de>>(
                            self,
                            mut map: A,
                        ) -> Result<Scheme, A::Error> {
                            let mut scheme: Option<String> = None;
                            let mut raw_config: Option<serde_value::Value> = None;

                            while let Some(key) = map.next_key::<String>()? {
                                match key.as_str() {
                                    "scheme" => {
                                        scheme = Some(map.next_value()?);
                                    }
                                    "config" => {
                                        raw_config = Some(map.next_value()?);
                                    }
                                    _ => {
                                        let _ = map.next_value::<serde::de::IgnoredAny>()?;
                                    }
                                }
                            }

                            let scheme = scheme.ok_or_else(|| {
                                serde::de::Error::missing_field("scheme")
                            })?;
                            let raw_config = raw_config.ok_or_else(|| {
                                serde::de::Error::missing_field("config")
                            })?;

                            $(
                                #[cfg(feature = $feature)]
                                if scheme == to_key(stringify!($variant)) {
                                    use serde::de::IntoDeserializer;
                                    let cfg: $config =
                                        serde::Deserialize::deserialize(
                                            raw_config.into_deserializer()
                                        )
                                        .map_err(serde::de::Error::custom)?;
                                    return Ok(Scheme::$variant(cfg));
                                }
                            )*

                            // Unrecognised scheme → Dynamic.
                            use serde::de::IntoDeserializer;
                            let config: std::collections::HashMap<String, String> =
                                serde::Deserialize::deserialize(
                                    raw_config.into_deserializer()
                                )
                                .map_err(serde::de::Error::custom)?;
                            Ok(Scheme::Dynamic { backend: scheme, config })
                        }
                    }

                    deserializer.deserialize_map(SchemeVisitor)
                }
            }
        }
    };
}
