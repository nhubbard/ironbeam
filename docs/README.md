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

The root CI workflow builds the site for pull requests. A push to `main`, or
a manual CI run on `main`, builds and deploys the same site to
https://nhubbard.github.io/ironbeam/. GitHub Pages is configured to use
GitHub Actions as its source. The workflow verifies that `docs/site/index.html`
exists before it uploads the generated artifact. The generated standalone
workflow was integrated into the existing CI workflow.
