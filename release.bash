#!/usr/bin/env bash

# Adapted from https://github.com/oklog/oklog/blob/master/release.fish

read -p "Release version number: " VERSION

REV="$(git rev-parse --short HEAD)"
echo "Tagging $REV as v$VERSION"
git tag --annotate v$VERSION -m "Release v$VERSION"
echo "Be sure to: git push --tags"
echo
echo

DISTDIR="dist/v$VERSION"
mkdir -p $DISTDIR

read -p "Build for os/arch: " PAIR

GOOS="$(echo $PAIR | cut -d'/' -f1)"
GOARCH="$(echo $PAIR | cut -d'/' -f2)"
BIN="$DISTDIR/fdb-gateway"
echo $BIN
env GOOS=$GOOS GOARCH=$GOARCH go build -o $BIN -ldflags="-X main.version=$VERSION"
