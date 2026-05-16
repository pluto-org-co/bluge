# ![Bluge](docs/bluge.png) Bluge

[![PkgGoDev](https://pkg.go.dev/badge/github.com/pluto-org-co/bluge)](https://pkg.go.dev/github.com/pluto-org-co/bluge)
[![Tests](https://github.com/pluto-org-co/bluge/workflows/Tests/badge.svg?branch=master&event=push)](https://github.com/pluto-org-co/bluge/actions?query=workflow%3ATests+event%3Apush+branch%3Amaster)
[![Lint](https://github.com/pluto-org-co/bluge/workflows/Lint/badge.svg?branch=master&event=push)](https://github.com/pluto-org-co/bluge/actions?query=workflow%3ALint+event%3Apush+branch%3Amaster)

modern text indexing in go - [blugelabs.com](https://www.blugelabs.com/)

## This Fork

This is a mono-repo fork of [bluge](https://github.com/blugelabs/bluge) maintained by [Pluto](https://github.com/pluto-org-co), optimized for high-throughput offline indexing workloads.

**Indexing performance:** ~75% faster than upstream on the full offline indexing pipeline (`WriterOffline` -- parallel document analysis, segment creation, and multi-pass merge), measured by indexing 1 million documents with 4 keyword fields each (15s → 5.9s).

This fork is optimized for multi-core server hardware and trades CPU utilization for indexing throughput -- peak CPU usage during batch indexing is expected and intentional.

Upstream bluge is a stable, well-maintained library. This fork exists to consolidate internal patches and performance work in one place -- it is not intended as a general-purpose replacement.

## License

This repository is dual-licensed.

- **Upstream code** (all commits by [blugelabs](https://github.com/blugelabs/bluge) and contributors prior to this fork) is licensed under the **Apache License 2.0**. See [`LICENSE`](LICENSE).

- **Fork contributions** (all commits by [Shoriwe (Antonio José Donis Hung)](https://github.com/Shoriwe), any member of [pluto-org-co](https://github.com/pluto-org-co), or any contributor who directly contributes to this fork) are licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**. See [`LICENSE_AGPL`](LICENSE_AGPL).

By submitting a contribution to this repository, you agree that your contribution will be licensed under the AGPL-3.0.

Copyright (C) 2024 Antonio José Donis Hung (Shoriwe) and contributors to this fork.

## Features

* Supported field types:
    * Text, Numeric, Date, Geo Point
* Supported query types:
    * Term, Phrase, Match, Match Phrase, Prefix
    * Conjunction, Disjunction, Boolean
    * Numeric Range, Date Range
* BM25 Similarity/Scoring with pluggable interfaces
* Search result match highlighting
* Extendable Aggregations:
    * Bucketing
        * Terms
        * Numeric Range
        * Date Range
    * Metrics
        * Min/Max/Count/Sum
        * Avg/Weighted Avg
        * Cardinality Estimation ([HyperLogLog++](https://github.com/axiomhq/hyperloglog))
        * Quantile Approximation ([T-Digest](https://github.com/caio/go-tdigest))

## Indexing

```go
    config := bluge.DefaultConfig(path)
    writer, err := bluge.OpenWriter(config)
    if err != nil {
        log.Fatalf("error opening writer: %v", err)
    }
    defer writer.Close()

    doc := bluge.NewDocument("example").
        AddField(bluge.NewTextField("name", "bluge"))

    err = writer.Update(doc.ID(), doc)
    if err != nil {
        log.Fatalf("error updating document: %v", err)
    }
```

## Querying

```go
    reader, err := writer.Reader()
    if err != nil {
        log.Fatalf("error getting index reader: %v", err)
    }
    defer reader.Close()

    query := bluge.NewMatchQuery("bluge").SetField("name")
    request := bluge.NewTopNSearch(10, query).
        WithStandardAggregations()
    documentMatchIterator, err := reader.Search(context.Background(), request)
    if err != nil {
        log.Fatalf("error executing search: %v", err)
    }
    match, err := documentMatchIterator.Next()
    for err == nil && match != nil {
        err = match.VisitStoredFields(func(field string, value []byte) bool {
            if field == "_id" {
                fmt.Printf("match: %s\n", string(value))
            }
            return true
        })
        if err != nil {
            log.Fatalf("error loading stored fields: %v", err)
        }
        match, err = documentMatchIterator.Next()
    }
    if err != nil {
        log.Fatalf("error iterator document matches: %v", err)
    }
```

## Repobeats

![Alt](https://repobeats.axiom.co/api/embed/0d7f8bc7927e15b07f1ae592eeff01811c5a2f80.svg "Repobeats analytics image")

## License

Apache License Version 2.0
