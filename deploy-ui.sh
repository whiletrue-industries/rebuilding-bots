#!/bin/bash
git checkout main && \
(git branch -D dist || true) && \
git checkout -b dist && \
(cd ui && \
rm .gitignore && \
npm run build && \
cp dist/brbots/browser/index.html dist/brbots/browser/404.html && \
cp CNAME dist/brbots/ || true && \
git add dist/brbots && \
git commit -m dist) && \
(git branch -D gh-pages || true) && \
git subtree split --prefix ui/dist/brbots -b gh-pages && \
git push -f origin gh-pages:gh-pages && \
git checkout main && \
git branch -D gh-pages && \
git branch -D dist && \
git checkout . 