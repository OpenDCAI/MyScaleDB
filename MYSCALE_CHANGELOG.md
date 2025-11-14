# MyScale Release Notes
## 2025

### [v2.1.0](http://gitlab.originhub.tech/database/clickhouse/-/tags/myscale-v2.1.0) - 2025-07-02

Features & Improvements

- Optimize Table Join By swap table
 [!3](http://gitlab.originhub.tech/database/clickhouse/-/merge_requests/3) ([Pang shanfeng](http://gitlab.originhub.tech/shanfengp)).
- improve benchmark client
 [!4](http://gitlab.originhub.tech/database/clickhouse/-/merge_requests/4) ([Pang shanfeng](http://gitlab.originhub.tech/shanfengp)).
- Support multiple vector indices on a single column
 [!6](http://gitlab.originhub.tech/database/clickhouse/-/merge_requests/6) ([Jianmei Zhang](http://gitlab.originhub.tech/jianmeiz)).
- Support multi-mode generate bitmap set
 [!7](http://gitlab.originhub.tech/database/clickhouse/-/merge_requests/7) ([Pang shanfeng](http://gitlab.originhub.tech/shanfengp)).
- Support Sparse Vector Search
 [!8](http://gitlab.originhub.tech/database/clickhouse/-/merge_requests/8) ([Libao Yang](http://gitlab.originhub.tech/LibaoYang)).

Fixs

- Fix stateful test fail due to network error
 [!9](http://gitlab.originhub.tech/database/clickhouse/-/merge_requests/9) ([Pang shanfeng](http://gitlab.originhub.tech/shanfengp)).
- Fix index build error due to use default server config
 [!10](http://gitlab.originhub.tech/database/clickhouse/-/merge_requests/10) ([Pang shanfeng](http://gitlab.originhub.tech/shanfengp)).
- Fix local integration tests
 [!12](http://gitlab.originhub.tech/database/clickhouse/-/merge_requests/12) ([Pang shanfeng](http://gitlab.originhub.tech/shanfengp)).
- Fatal error in full_text_search table function
 [!13](http://gitlab.originhub.tech/database/clickhouse/-/merge_requests/13) ([Jianmei Zhang](http://gitlab.originhub.tech/jianmeiz)).
- Fix join undefined behavior test error
 [!16](http://gitlab.originhub.tech/database/clickhouse/-/merge_requests/16) ([Pang shanfeng](http://gitlab.originhub.tech/shanfengp)).
- Fix init part fatal when checksum error
 [!18](http://gitlab.originhub.tech/database/clickhouse/-/merge_requests/18) ([Pang shanfeng](http://gitlab.originhub.tech/shanfengp)).
- Fix the sparse index reader release
 [!21](http://gitlab.originhub.tech/database/clickhouse/-/merge_requests/21) ([Libao Yang](http://gitlab.originhub.tech/LibaoYang)).

### [v2.0.0] - 2025-04-02

- Upgrade ClickHouse to v24.8.8.1-lts

## 2024

### [v1.9.0](https://git.moqi.ai/mqdb/ClickHouse/-/tags/myscale-v1.9.0) - 2024-11-07

Features & Improvements

- Refactor the code for performance improvements and further optimizations.
 [!489](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/489) ([Shanfeng Pang](https://git.moqi.ai/shanfengp)).
- Remove the dependency on the Intel MKL library
 [!495](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/495) ([Shanfeng Pang](https://git.moqi.ai/shanfengp)).
- Improve performance for lightweight delete and merge
 [!498](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/498) ([Jianmei Zhang](https://git.moqi.ai/jianmeiz)).
- Optimize concurrent bitmap filter set
 [!499](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/499) ([Jianmei Zhang](https://git.moqi.ai/jianmeiz)).
- Optimize slow lightweight delete
 [!502](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/502) ([Jianmei Zhang](https://git.moqi.ai/jianmeiz)).
- Add other registry source to Cargo
 [!503](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/503) ([Shanfeng Pang](https://git.moqi.ai/shanfengp)).
- Properly initialize two stage variables added for multiple distances
 [!504](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/504) ([Jianmei Zhang](https://git.moqi.ai/jianmeiz)).

Fixs

- Fix fatal when prewhere and where coexist
 [!500](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/500) ([Libao Yang](https://git.moqi.ai/libaoy)).
- Fix typo in FTS acceleration setting
 [!506](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/506) ([Libao Yang](https://git.moqi.ai/libaoy)).


### [v1.8.0](https://git.moqi.ai/mqdb/ClickHouse/-/tags/myscale-v1.8.0) - 2024-09-24

Features & Improvements

- Add support for multiple `distance()` functions in a single query.
 [!491](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/491) ([Jianmei Zhang](https://git.moqi.ai/jianmeiz)).
- Resolve score calculation error in distributed hybrid search.
 [!492](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/492) ([Libao Yang](https://git.moqi.ai/libaoy)).
- Improve filterd vector search performance by intrudcing a setting for parallel reading in `performPrefilter()`.
 [!493](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/493) ([Jianmei Zhang](https://git.moqi.ai/jianmeiz)).
- Enhance `TextSearch()` and `HybridSearch()` to support multi-column FTS indexing。
 [!496](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/496) ([Jianmei Zhang](https://git.moqi.ai/jianmeiz)).


### [v1.7.1](https://git.moqi.ai/mqdb/ClickHouse/-/tags/myscale-v1.7.1) - 2024-09-02

Features & Improvements

- Improve automated release scripts.
 [!476](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/476) ([Shanfeng Pang](https://git.moqi.ai/shanfengp)).
- Fix CI for label detection.
 [!481](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/481) ([Qin Liu](https://git.moqi.ai/qliu)).
- Implement parallel reading in `performPrefilter()`.
 [!484](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/484) ([Jianmei Zhang](https://git.moqi.ai/jianmeiz)).
- Update AMI product code.
 [!485](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/485) ([Shanfeng Pang](https://git.moqi.ai/shanfengp)).
- Optimize filter search for large datasets.
 [!478](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/478) ([Qin Liu](https://git.moqi.ai/qliu)).

Fixs

- Resolve the "failed to load Tantivy index file" error when deleting parts.
 [!486](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/486) ([Mochi Xu](https://git.moqi.ai/mochix)).
- Correct distance results for Cosine distance in two-stage vector search.
 [!482](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/482) ([Shanfeng Pang](https://git.moqi.ai/shanfengp)).
- Resolve the tantivy index loading error.
 [!483](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/483) ([Mochi Xu](https://git.moqi.ai/mochix)).

### [v1.7.0](https://git.moqi.ai/mqdb/ClickHouse/-/tags/myscale-v1.7.0) - 2024-08-19

Features & Improvements

- Add support for full-text search across multiple text columns.
 [!470](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/470) ([Jianmei Zhang](https://git.moqi.ai/jianmeiz)).
- Optimize queries per second (QPS) for TextSearch during inserts.
 [!473](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/473) ([Mochi Xu](https://git.moqi.ai/mochix)).

Fixs

- Correct BM25 calculation error in distributed text search.
 [!471](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/471) ([Libao Yang](https://git.moqi.ai/libaoy)).
- Resolve error when performing hybrid search on distributed tables.
 [!472](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/472) ([Jianmei Zhang](https://git.moqi.ai/jianmeiz)).
- Fix segmentation fault when executing parallel text search selects with FINAL.
 [!474](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/474) ([Jianmei Zhang](https://git.moqi.ai/jianmeiz)).
- Address various bugs in the full-text search function.
 [!475](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/475) ([Jianmei Zhang](https://git.moqi.ai/jianmeiz)).
- Fix a critical issue with removing the FTS index cache directory.
 [!477](https://git.moqi.ai/mqdb/ClickHouse/-/merge_requests/477) ([Mochi Xu](https://git.moqi.ai/mochix)).


