"""Check the generated Pages artifact, including anchors and the project base path."""

from html.parser import HTMLParser
from pathlib import Path
import re
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parent / ".vitepress/dist"
BASE = "/mac-worker/"


class Page(HTMLParser):
    def __init__(self, path):
        super().__init__()
        self.links = []
        self.ids = set()
        self.feed(path.read_text(encoding="utf-8"))

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if "id" in attrs:
            self.ids.add(attrs["id"])
        for name in ("href", "src"):
            if name in attrs:
                self.links.append(attrs[name])


pages = {path.resolve(): Page(path) for path in ROOT.rglob("*.html")}
assert pages, "Build the documentation first."
resources = {path: page.links for path, page in pages.items()}
for stylesheet in ROOT.rglob("*.css"):
    resources[stylesheet.resolve()] = re.findall(r"url\(\s*['\"]?([^\s)'\"]+)['\"]?\s*\)", stylesheet.read_text(encoding="utf-8"))
errors = set()
checked = 0
for source, links in resources.items():
    for link in links:
        url = urlsplit(link)
        if url.scheme or url.netloc:
            continue
        pathname = unquote(url.path)
        if pathname.startswith("/"):
            if not pathname.startswith(BASE):
                errors.add(f"{source.relative_to(ROOT)}: outside Pages base: {link}")
                continue
            target = ROOT / pathname[len(BASE):]
        elif pathname:
            target = source.parent / pathname
        else:
            target = source
        target = target.resolve()
        if target.is_dir():
            target /= "index.html"
        if not target.exists():
            errors.add(f"{source.relative_to(ROOT)}: missing file: {link}")
        elif url.fragment and target in pages and unquote(url.fragment) not in pages[target].ids:
            errors.add(f"{source.relative_to(ROOT)}: missing anchor: {link}")
        checked += 1

for error in sorted(errors):
    print(error)
assert not errors, f"{len(errors)} broken site links"
print(f"Checked {len(pages)} pages and {checked} internal links/assets, including anchors.")
