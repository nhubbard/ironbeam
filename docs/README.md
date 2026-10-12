# Ironbeam documentation site

This Zensical project was initialized independently, then added under `docs/`.
The site sources are in `docs/docs/` and the project configuration is
`docs/zensical.toml`.

Install Zensical in an isolated environment and run from this folder:

```sh
python3 -m venv .venv
.venv/bin/pip install 'zensical==0.0.69'
.venv/bin/zensical build --strict --clean
.venv/bin/zensical serve
```

The root CI workflow builds the site for pull requests and deploys it to
GitHub Pages on pushes to `main`. The generated standalone workflow was
integrated into the existing CI workflow.
