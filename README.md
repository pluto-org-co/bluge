# ![Bluge](docs/bluge.png) Bluge

[![PkgGoDev](https://pkg.go.dev/badge/github.com/pluto-org-co/bluge)](https://pkg.go.dev/github.com/pluto-org-co/bluge)
[![Tests](https://github.com/pluto-org-co/bluge/workflows/Tests/badge.svg?branch=master&event=push)](https://github.com/pluto-org-co/bluge/actions?query=workflow%3ATests+event%3Apush+branch%3Amaster)
[![Lint](https://github.com/pluto-org-co/bluge/workflows/Lint/badge.svg?branch=master&event=push)](https://github.com/pluto-org-co/bluge/actions?query=workflow%3ALint+event%3Apush+branch%3Amaster)

modern text indexing in go - [blugelabs.com](https://www.blugelabs.com/)

## This Fork

This is a mono-repo fork of [bluge](https://github.com/blugelabs/bluge) maintained by [Pluto](https://github.com/pluto-org-co), optimized for high-throughput offline indexing workloads.

### What changed

The upstream library was architecturally modeled after Java OOP patterns — a separate `bluge_segment_api` package defining speculative interfaces with a single implementation, getter/setter methods on all field types, and pervasive interface boxing throughout the write path. In Go, this pattern has concrete costs: every interface call is an indirect dispatch the compiler cannot inline, every boxed value is a heap allocation the GC must track, and the compiler's escape analysis is blind to concrete types hidden behind interfaces.

This fork addresses those problems at the root:

- **Mono-repo consolidation** — `bluge`, `bluge_segment_api`, and all internal packages collapsed into a single module, enabling cross-package inlining and atomic refactoring
- **`bluge_segment_api` removed entirely** — the speculative interface layer had one implementation and zero external implementors; it was pure overhead
- **All field types made concrete** — `KeywordField`, `TextField`, `NumericField` and all others are now concrete structs with public fields, no interface receivers, no getters or setters
- **Offline writer redesigned** — `OfflineWriter` now accepts `segmentSize` and `workers` parameters, replacing the original all-or-nothing batch model

### Performance

Benchmark: 1,000,000 documents × 4 keyword fields each (`_id`, `name`, `index`, `reversed-name`), Intel i9-10900K, linux/amd64, `go test -bench -benchmem -count 5`. All numbers are averages across 5 runs.

#### vs upstream — Writer

| | upstream | this fork | delta |
|---|---|---|---|
| time | 12,187 ms | 9,148 ms | **−25% / 1.33× faster** |
| memory | 8,204 MB | 4,722 MB | **−42%** |
| allocs/op | 131,033,474 | 56,233,336 | **−57%** |

#### vs upstream — OfflineWriter

| | upstream | this fork | delta |
|---|---|---|---|
| time | 14,283 ms | 5,004 ms | **−65% / 2.85× faster** |
| memory | 9,291 MB | 6,345 MB | **−32%** |
| allocs/op | 185,834,990 | 104,854,713 | **−44%** |

#### vs bleve

Bleve has no dedicated offline writer — `BenchmarkOfflineWriter` uses `bleve.NewUsing` with scorch/zap segment hints, the closest equivalent. This fork's `OfflineWriter` is compared against both bleve variants.

| | bleve | this fork (OfflineWriter) | delta |
|---|---|---|---|
| time (OfflineWriter) | 24,007 ms | 5,004 ms | **−79% / 4.80× faster** |
| memory (OfflineWriter) | 10,070 MB | 6,345 MB | **−37%** |
| allocs/op (OfflineWriter) | 146,542,599 | 104,854,713 | **−28%** |
| time (Writer) | 25,133 ms | 5,004 ms | **−80% / 5.02× faster** |
| memory (Writer) | 10,459 MB | 6,345 MB | **−39%** |
| allocs/op (Writer) | 158,542,972 | 104,854,713 | **−34%** |

#### OfflineWriter vs Writer (this fork, 1M documents)

| variant | time | memory | allocs/op |
|---|---|---|---|
| `Writer` | 9,148 ms | 4,722 MB | 56.2M |
| `OfflineWriter` | 5,004 ms | 6,345 MB | 104.9M |

`OfflineWriter` is ~45% faster than `Writer` for bulk ingestion by parallelising segment construction across workers. The tradeoff is higher peak memory and more allocations — it buffers segments in memory before flushing rather than streaming incrementally. For batch indexing workloads where throughput matters, `OfflineWriter` is the correct choice. For live indexing with concurrent reads, use `Writer`.

#### How the gains were achieved

| change | time impact | alloc impact |
|---|---|---|
| Write path optimization + `segmentSize`/`workers` exposure | −64% | −18% |
| `bluge_segment_api` removal + concrete types | −12% | −20% |
| Public fields, incremental cleanup | ~flat | −6% |
| Analyzer interface removal + memory allocation improvements | ~flat | −8% |
| **total (OfflineWriter vs upstream)** | **−65%** | **−44%** |
| **total (Writer vs upstream)** | **−25%** | **−57%** |

The allocation reduction is the most meaningful number — it is hardware-independent and noise-resistant. The Writer path in particular dropped from 131M to 56M allocs, a reduction of 75 million allocations per operation.

### New APIs

```go
// segmentSize controls how many documents are buffered per segment before flush
// workers controls how many segments are built in parallel
writer, err := bluge.OpenOfflineWriter(config, 50_000, 10)

// batch insert
err = writer.Batch(batch)

// FieldDefinition pattern — zero overhead vs direct field construction
info, fields := bluge.FieldsFromDefinitions(
    bluge.NewKeywordFieldDefinition("name", "hello"),
    bluge.NewKeywordFieldDefinition("status", "active"),
)
doc := bluge.NewDocumentWithFields(id, info, fields...)

// managed ID variant
info, fields := bluge.FieldsFromDefinitionsWithId(id,
    bluge.NewKeywordFieldDefinition("name", "hello"),
)
doc := bluge.NewDocumentWithFieldsManagedId(info, fields...)
```

### Scope and intent

This fork is optimized for multi-core server hardware and trades peak memory for indexing throughput — sustained high CPU usage during batch indexing is expected and intentional.

The upstream library had its last commit in 2021. This fork exists to consolidate internal patches, remove accumulated abstraction debt, and restore the library to production fitness for high-volume indexing workloads. It is not intended as a general-purpose drop-in replacement — the public API has changed in breaking ways (field types are no longer interface values, getters are gone).

The read path, search path, and segment merge path have not yet been profiled or optimized. Current gains are entirely on the write path.

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
        * Cardinality Estimation ([HyperLogLog++](https://github.com/axiomhz/hyperloglog))
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
