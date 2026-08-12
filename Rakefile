# each test file configures SearchkickMultitenant globally with a
# different tenancy strategy (schema-based vs row-based) to demonstrate
# both are supported — exactly one strategy per real app/process, so
# these can't be loaded together in a single Rake::TestTask process.
# Run each as its own subprocess instead.
task :test do
  files = FileList["test/**/*_test.rb"]
  failures = files.reject { |f| system("bundle", "exec", "ruby", "-Itest", f) }
  raise "Failures in: #{failures.join(", ")}" if failures.any?
end

task default: :test
