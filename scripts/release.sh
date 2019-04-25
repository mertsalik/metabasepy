#!/usr/bin/env bash

: ${1?"Usage: $0 release_version (ex: 1.9 or 2.4 etc) "}

cd "$(dirname "$0")"

scripts_dir=`pwd`
project_dir="$(dirname "$scripts_dir")"

echo $1 > "$project_dir"/VERSION
git tag -a "$1"
git push --tag

cd "$project_dir"

rm -rf dist/*
python setup.py bdist_wheel --universal
twine check dist/*
python -m twine upload dist/*

git commit -am "new release $1"
git push origin master
