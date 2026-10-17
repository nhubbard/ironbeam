# Ironbeam compression

`ironbeam-compression` provides streaming compression and decompression by
filename, magic bytes, or registered codecs. Enable only the formats used by
your application: `compression-gzip`, `compression-zstd`,
`compression-bzip2`, or `compression-xz`.

The crate has no cloud provider or file-format dependency. V5 file I/O and
cloud JSONL use it directly. The old `ironbeam_cloud::compression` path is
available from published Ironbeam 4.1.1 during migration.
