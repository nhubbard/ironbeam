# Ironbeam compression

`ironbeam-compression` provides streaming compression and decompression by
filename, magic bytes, or registered codecs. Enable only the formats used by
your application: `compression-gzip`, `compression-zstd`,
`compression-bzip2`, or `compression-xz`.

The crate has no cloud provider or file-format dependency. Existing
`ironbeam_cloud::compression` imports remain available through a migration
reexport, while v5 file I/O depends on this crate directly.
