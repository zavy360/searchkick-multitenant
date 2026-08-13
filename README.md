# searchkick-multitenant

First-class multitenancy for [Searchkick](https://github.com/ankane/searchkick) with **one shared
Elasticsearch/OpenSearch index per model**, instead of Searchkick's stock recommendation of one
index per tenant per model (which doesn't scale past a few dozen tenants).

Every document gets a `tenant` field and a collision-safe composite `_id` (`"tenant::id"`), so two
tenants can legitimately have a record with the same primary key (common with schema-per-tenant
setups like [Apartment](https://github.com/rails-on-services/apartment)) without colliding in the
shared index. Search, indexing, and reindexing are all tenant-aware — `Model.search`/`Model.reindex`
work exactly as they do in stock Searchkick, just scoped correctly per tenant automatically.

## Status

The core write/read/search path and every reindex path (single-record callbacks, `:queue` mode,
`:async` mode, and a full "reindex every tenant, promote once" flow) are implemented and covered by
tests against a real OpenSearch instance, plus a full audit of every Searchkick code path that reads
or writes the index (see [Other safety guards](#other-safety-guards)). It has not been used in
production. Read [How it works](#how-it-works) before relying on it — several of the hooks are
`Module#prepend`s onto Searchkick internals that aren't public extension APIs, so they're coupled to
Searchkick's current implementation and may need adjustment on a Searchkick version bump.

## Getting started

```ruby
gem "searchkick-multitenant"
```

**1. Configure the two app-wide settings**, usually in an initializer:

```ruby
Searchkick::MultiTenant.configure do |c|
  # required: how to read the current tenant (used for the query-time ES filter)
  c.current_tenant = -> { Apartment::Tenant.current }

  # required: how to iterate every tenant, for full reindexing
  c.each_tenant = ->(&block) { Apartment::Tenant.each(&block) }

  # use the tenant as the ES `_routing` value (recommended: keeps a
  # tenant's documents on the same shard)
  c.routing = true
end
```

**2. Add two methods to each tenant-scoped model**, after the existing `searchkick` call:

```ruby
class Ticket < ApplicationRecord
  searchkick word_start: [:name]
  searchkick_multitenant tenant: :account_id, tenant_scope: ->(tenant, &block) { block.call(where(account_id: tenant)) }
end
```

That's it for a row-based model — `tenant:` says how to read the tenant off a record, `tenant_scope:`
says how to scope a relation to one tenant. See [below](#configuring-a-model) for the schema-based
(Apartment) shape and the other ways to define these two methods.

**3. Use `Model.search`/`Model.reindex` exactly as normal** — they're tenant-scoped automatically:

```ruby
Ticket.reindex               # reindexes every tenant into the shared index, promotes once
Ticket.search("login issue") # scoped to Searchkick::MultiTenant.current_tenant automatically
```

## Configuring a model

Two things are configured per model, following Searchkick's own convention for customization
(`index_name:`, `search_document_id`, `should_index?`): pass a proc, pass a symbol naming a method,
or just define the method directly.

**`searchkick_tenant_scope(tenant, &block)`** — a class method. Owns building *and* yielding a
scoped relation for `tenant`, as one atomic step (so there's no separate "switch context" step that
can be composed in the wrong order):

```ruby
# row-based
class Ticket < ApplicationRecord
  searchkick word_start: [:name]
  searchkick_multitenant tenant: :account_id, tenant_scope: ->(tenant, &block) { block.call(where(account_id: tenant)) }
end

# schema-based (Apartment), defined directly instead of passed to the macro
class Product < ApplicationRecord
  searchkick word_start: [:name]
  searchkick_multitenant

  def self.searchkick_tenant_scope(tenant, &block)
    Apartment::Tenant.switch(tenant) { block.call(all) }
  end
end
```

There's no built-in default for this — every tenant-enabled model must define it (it's one line
either way). This used to be a `tenant_column:` shortcut with an implicit fallback; that's gone in
favor of always being explicit.

**`searchkick_tenant`** — an instance method, "what tenant does this record belong to" (used to tag
the document on write). Defaults to the ambient `current_tenant` when not configured — correct by
construction for schema-based tenancy (a record can only have been loaded/created while switched
into its own tenant's schema). Row-based models override it with the column:

```ruby
searchkick_multitenant tenant: :account_id # or tenant: ->{ account.subdomain }, or just define #searchkick_tenant
```

## How it works

| Concern | Hook |
|---|---|
| Collision-safe `_id` + routing on write | `Module#prepend` on `Searchkick::RecordData` (`search_id`, `record_data`) |
| `tenant` field in the indexed document | `search_data` override on the model (same extension point Searchkick's own default `search_data` uses) |
| Reversing the composite `_id` back to a real DB id on read | `Module#prepend` on `Searchkick::Results` (`results_query`, `with_hit_and_missing_records`) |
| Automatic `where: {tenant: current_tenant}` on every search | `Module#prepend` on `Searchkick.search` |
| `:queue` reindex mode (one Redis list shared by every tenant) | `Module#prepend` on `Searchkick::ReindexQueue`/`ProcessQueueJob`/`ProcessBatchJob`, adding a 3rd pipe-encoded field |
| `:async` callback mode + full reindex's own async mode | `Module#prepend` on `Searchkick::ReindexV2Job`/`BulkReindexJob` (`serialize`/`deserialize`/`perform`) |
| `Model.reindex`/`rake searchkick:reindex[:all]` covering every tenant, promoted once | `Module#prepend` on `Searchkick::Index#full_reindex`, delegating to `Searchkick::MultiTenant::TenantReindexer` (composed from `Index`'s existing public `create_index`/`import_scope`/`promote`/`clean_indices`) |

`:inline` callback mode needs no gem code beyond the write/read hooks above: it runs synchronously
in the same request, so the ambient tenant is always correct.

`:async` mode is **not** that simple, and does need the prepend above — Searchkick's own
`RecordIndexer#reindex_items` has a real orphan-cleanup step: if a job's target id isn't found in
the DB, it's treated as deleted and removed from the index via a bare reconstructed record
(`id` only, no other attributes). Without capturing which tenant a job was *actually* about at
enqueue time, two things go wrong: for schema-based tenancy, a worker thread that's moved on to a
different tenant by the time the job runs would resolve that reconstructed record's tenant to
whatever's *currently* ambient — not the tenant the job was enqueued for — and could delete a
different tenant's valid document that happens to share the same id. For row-based tenancy, the
reconstructed record has no tenant column value at all, so the delete targets the wrong composite
id outright and the real orphaned document is never cleaned up. The fix captures the tenant via
ActiveJob's `serialize` (which runs synchronously at enqueue time, in whatever context is actually
correct — a record save, or the reindex-every-tenant loop) and threads it through
`searchkick_tenant_scope` instead of the bare unscoped class. `test/async_job_test.rb` has the
regression test: enqueue a destroy-reindex under one tenant, simulate the ambient tenant drifting
to a different one before the job executes, and assert only the correct tenant's document is
affected.

## Escape hatch

Cross-tenant/admin search:

```ruby
Searchkick.without_tenant_scope { Product.search("widget") }
```

## Multi-model search

`Searchkick.search(term, models: [Product, Category])` is tenant-scoped automatically, same as a
single-model search, as long as every model in the list is tenant-enabled and uses the same tenant
field name. If some models are enabled and some aren't, or enabled models use different tenant
field names, this raises `Searchkick::MultiTenant::Error` rather than silently searching part of the
list unscoped — pass `where:` explicitly or `skip_tenant_scope: true` for that case.

## Other safety guards

A full audit of every Searchkick code path that reads or writes the index turned up four more
places that needed a guard, beyond the write/read/search/reindex hooks in the table above:

- **Bare `Model.reindex` transparently covers every tenant** for tenant-enabled models
  (`Module#prepend` on `Searchkick::Index#full_reindex`) — including `rake searchkick:reindex`/
  `searchkick:reindex:all`, which call it the same way. Without this, it would silently rebuild the
  shared index from only whichever tenant is ambient at call time and promote over every other
  tenant's data. Rather than raise and push a different API onto every caller, it redirects to
  `TenantReindexer.call` under the hood, so the standard interface Just Works. The handful of
  options `TenantReindexer` has no equivalent for (`import: false`, `refresh_interval:`, `scope:`,
  `wait:`) still raise rather than being silently dropped — call `TenantReindexer.call` directly for
  that level of control (see [Advanced: TenantReindexer](#advanced-tenantreindexer)).
- **`Relation#rewhere`/`#only`/`#except` reapply the tenant filter.** The query-time `where:`
  injection happens once, in `Searchkick.search`, before `Relation.new` is constructed — but
  `rewhere!` unconditionally replaces `@options[:where]` wholesale, and `only`/`except` rebuild via
  a direct `Relation.new` call that never goes back through `Searchkick.search` at all. Any of the
  three silently dropped the tenant filter with no re-injection. `Module#prepend` on
  `Searchkick::Relation` reapplies it afterward via the same `where!` used everywhere else (merges
  safely rather than risking another wholesale replace).
- **`suggest:` is rejected** for tenant-enabled models. Searchkick builds the ES `suggest` section
  independently of the query's `where:`/filter context, so it would surface suggestion terms
  sourced from every tenant regardless of the injected filter — there's no per-tenant suggester
  context plumbed by Searchkick to scope this correctly, so it's refused rather than silently
  leaking. Use `skip_tenant_scope: true` if a search is intentionally cross-tenant.
- **Aggregating directly on the tenant field is rejected.** Searchkick's `smart_aggs` (on by
  default) deliberately excludes a field's own condition from *that field's* aggregation bucket, for
  full facet counts. If the aggregated field is the tenant field itself, that exclusion drops the
  tenant filter from the bucket, leaking other tenants' bucket keys/counts even though `hits` stays
  correctly scoped. Aggregating any other field is unaffected. Pass `smart_aggs: false` if this is
  intentional.

Checked and confirmed **not** a gap: `record.similar` (goes through `Searchkick.search` like any
other query), `Index#retrieve` (id-based single-doc fetch, no query/where involved),
`Searchkick.multi_search` (can only batch `Query` objects that already passed through
`Searchkick.search`), the thread-local bulk indexer queue (each item's composite id/tenant tag is
fully realized at queue time, not recomputed from ambient state at flush time — a mixed-tenant batch
is safe), `reranking.rb` (pure result-reordering, issues no query of its own), and general
`where:` hash merging (top-level keys always AND, `_or`/`_and` sub-clauses don't escape the
enclosing filter array).

## Advanced: TenantReindexer

`Model.reindex` delegates to `Searchkick::MultiTenant::TenantReindexer` under the hood, so you don't
need to call it directly for normal use. Reach for it when you need control `Model.reindex` doesn't
expose:

**Resuming a failed reindex.** A tenant's import is retried a few times before being treated as
failed; if any tenant is still outstanding, the run raises `Searchkick::MultiTenant::PartialReindexError`
**without promoting** — the new index is left in place (not cleaned up) so a resumed run only
retries what's outstanding:

```ruby
Searchkick::MultiTenant::TenantReindexer.call(Product, resume: true)
```

**Backfilling one tenant** (e.g. newly onboarded) into the *currently promoted* index, without a new
index or promotion:

```ruby
Searchkick::MultiTenant::TenantReindexer.reindex_tenant(Product, "new-tenant-id")
```

Both require `Searchkick.redis` to be configured (used to checkpoint which tenants are still
outstanding).

## Known limitations

- `force_promote_incomplete: true` on `TenantReindexer.call` will promote an index missing one or
  more tenants' data. It exists as a documented, explicit escape hatch — it is never the default,
  and every use is logged.

## Development

```
bundle install
docker run -d -p 9200:9200 -e "discovery.type=single-node" -e "plugins.security.disabled=true" opensearchproject/opensearch:2
redis-server &
rake test
```
