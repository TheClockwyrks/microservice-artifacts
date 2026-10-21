# history

The record of how this repository's history was extracted from the monorepo,
[The Test Cabinet](https://github.com/TheClockwyrks/TheTestCabinet), where the
artifacts crate lived until the extraction. Nothing builds or tests against
these files; they are kept so the provenance outlives the scratch clone the
extraction ran in.

## The extraction

The history below the scaffold commit is the monorepo's `issue/repos-14` at
`ba18562a79fd2941543611dc079371fa39670a40` (its `nightly` plus the commit that
gave the backend a copy of the R2 client, so that no other crate of the
monorepo depended on this one), filtered to the paths in
[`paths.txt`](paths.txt) by git filter-repo 2.47.0:

```sh
git clone -q --no-local --single-branch --branch issue/repos-14 --no-tags \
  <the-test-cabinet> x
cd x
uvx --from git-filter-repo==2.47.0 git-filter-repo --force \
  --paths-from-file ../paths.txt --prune-empty always --preserve-commit-hashes
```

Of the 7,059 commits reachable from that commit, 115 touched a path in the
list and were kept (26 of them merges); the rest were pruned. No tag was
carried. The tip trees of `crates/artifacts/` and `.env.artifacts.example` hash
the same as the monorepo's at that commit:

```text
crates/artifacts         973dc4f5be802d647b2eb91479f0d7f328d1d7ec
.env.artifacts.example   f4fa3f095c0642cd03e3c52d598427289114db04
```

The paths are kept where they were in the monorepo, so the filter needed no
rename.

## The files

- [`paths.txt`](paths.txt) is the list handed to `--paths-from-file`
  (sha256 `a102820f1588fffdf0473bd53f2f9126ceba33abdeb12b63e630282bbd0ee5f4`):
  the crate's directory, `crates/artifacts/`, the environment example,
  `.env.artifacts.example`, and every path a tracked file under them had before
  a rename or copy, collected by `git log --follow --name-status -M -C` over
  each of the 44 files, so a file's history reaches back past the commits that
  moved it into place.
- [`commit-map`](commit-map) is filter-repo's map from each monorepo commit
  (left) to the commit it became here (right), all zeros for a pruned one.
  `--preserve-commit-hashes` left the commit ids quoted in the messages as they
  were, so an id in a message below the scaffold commit names a monorepo
  commit, and this map translates it.

## The live-path review

`paths.txt` names ten paths outside the moved set, each the path a file here
had before it was moved or copied into place:

- `crates/core/src/playable.rs` and its test, and `crates/core/src/r2.rs` and
  its test: the monorepo's run engine before it was renamed the driver, where
  the playable resolvers and the R2 client began.
- `crates/worker/src/error.rs`: the error type `src/error.rs` began as a copy
  of.
- `crates/backend/src/r2.rs` and its test: the R2 client while the backend
  held it, before it moved here.
- `crates/backend/src/access.rs`, `access.test.rs` and `access.keys.test.rs`:
  the backend's Cloudflare Access verification, which `src/access.rs` and its
  tests began as a copy of.

The `crates/core` and `crates/worker` paths are not live in the monorepo's
tree at the extraction commit. The five `crates/backend` paths are: the
backend keeps its own Access verification, and the extraction commit gave it
back an interim copy of the R2 client. The filtered tip therefore carried
those five files, and the scaffold commit deletes them, so no file of the
backend is part of this repository's tree.
