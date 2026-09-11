# Documentation website

The documentation is published at [kirchik95.github.io/mac-worker](https://kirchik95.github.io/mac-worker/).
GitHub Actions builds and deploys it when documentation changes reach `main`.
Pull requests build the site and check its links without deploying it.

## Edit the documentation

Keep writing Markdown in `docs/`. Keep `README.md` and `README.ru.md` in sync.
The website generates its English and Russian quick starts from those two
files. The complete guides currently remain in their original language;
the Russian navigation labels the English guides explicitly.

`docs-site/prepare.mjs` copies the documentation into an ignored build
directory and rewrites repository links for the website. Code examples are
preserved. The complete document index includes design notes, plans and
validation records; those pages display an archive notice so old plans are
not mistaken for current behavior. The original files remain readable on
GitHub.

Do not edit or commit `docs-site/content/` or `docs-site/.vitepress/dist/`.
Edit navigation and theme settings in `docs-site/.vitepress/config.mjs`.

## Build and preview locally

Use Node.js 24 and npm. Python 3 is used by the static link check.

```bash
cd docs-site
npm ci --ignore-scripts
npm run build
python3 check-links.py
npm run preview -- --port 4173
```

Open `http://127.0.0.1:4173/mac-worker/`. The `/mac-worker/` prefix matches
GitHub Pages. For a development server, run `npm run dev`; rerun
`npm run prepare` after editing the source Markdown outside `docs-site/content`.

The site uses VitePress's local search, so it needs no search account or
server. Its IBM Plex fonts and their OFL license are copied from the existing
dashboard assets. Mermaid diagrams are rendered by the pinned site dependencies.

## Publish and recover

The `Documentation` workflow in `.github/workflows/docs.yml` uploads only the
built static site. It uses GitHub Pages with GitHub Actions as the publishing
source, the `github-pages` environment, and deployment permissions restricted
to the deploy job on `main`.

A failed build or link check prevents deployment. After correcting the source,
push the fix or rerun the workflow. To restore a previous documentation version,
revert the relevant documentation commit and let the same workflow deploy it.
No worker binary installation or controller restart is involved.
