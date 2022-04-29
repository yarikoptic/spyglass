# Spyglass Documentation

## Introduction

This documentaton is a Jupyter Book. However, it uses Sphinx functionality. Files can be either .md or .rst files.

## Generating Documentation

- Go to `docs/`
- Run - `pip install -r requirements.txt`
- Run -  `make clean && make html`

Notes.
- Sphinx will crash and may not complete html generation if any Python docstring is not valid numpydoc (see - https://discourse.matplotlib.org/t/documentation-build-error/12036/2). You will get an error like "(SEVERE/4) Unexpected section title" due to this.
- Pandoc cannot be installed with pip. It has to be downloaded with conda or some other means. See - https://stackoverflow.com/a/75132769/178550

After completion, go to - /docs/html. Open `index.html` in a browser.
