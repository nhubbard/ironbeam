#[cfg_attr(docsrs, doc(cfg(feature = "io-jsonl")))]
#[cfg(feature = "io-jsonl")]
pub mod jsonl;

#[cfg_attr(docsrs, doc(cfg(feature = "io-csv")))]
#[cfg(feature = "io-csv")]
pub mod csv;

#[cfg_attr(docsrs, doc(cfg(feature = "io-parquet")))]
#[cfg(feature = "io-parquet")]
pub mod parquet;
