module Searchkick
  module MultiTenant
    # One tenant's import_tenant, run as its own background job — this is
    # what actually gives TenantReindexer's mode: :async concurrency across
    # tenants. A raw thread pool inside TenantReindexer#call was the first
    # attempt at this, but manually spawned threads don't get their DB
    # connection released back to the pool the way Rails' own request/job
    # executor wrapping does for a thread it manages itself — every
    # TenantReindexer.call would leak `concurrency` connections. Sidekiq/
    # ActiveJob already checks a connection out and back in correctly per
    # job, so running each tenant as a real job sidesteps needing to
    # reimplement that lifecycle here at all. Concurrency is then just however
    # many workers Sidekiq itself runs — no separate knob in this gem.
    #
    # mode: :inline never reaches this — it has no Sidekiq dependency today
    # (import_tenant runs straight in the calling process) and this doesn't
    # introduce one; only mode: :async's tenant loop is background-job-based.
    class TenantImportJob < Searchkick.parent_job.constantize
      queue_as { Searchkick.queue_name }

      def perform(class_name:, tenant:, new_index_name:, tenants_key:, mode:, job_options: nil, scope: nil)
        model = Searchkick.load_model(class_name)
        new_index = Searchkick::Index.new(new_index_name, model.searchkick_options)
        reindexer = TenantReindexer.new(model, mode: mode, job_options: job_options, scope: scope)

        reindexer.import_tenant(new_index, tenant)
        reindexer.remove_from_checkpoint(tenants_key, tenant)
      rescue => e
        # not a give-up-after-N-attempts scheme like BulkReindexJobExt's —
        # leaving the tenant in tenants_key (not removed) is exactly what
        # already lets PartialReindexError/resume: true pick it back up, so
        # there's nothing extra to unblock here. Re-raise so the job still
        # shows up failed in Sidekiq for visibility, same reasoning as
        # BulkReindexJobExt.
        Searchkick.warn("[searchkick-multitenant] tenant #{tenant.inspect} failed to reindex into #{new_index_name}: #{e.message}")
        raise
      end
    end
  end
end
