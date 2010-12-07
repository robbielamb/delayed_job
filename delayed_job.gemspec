#version = File.read('README.textile').scan(/^\*\s+([\d\.]+)/).flatten

Gem::Specification.new do |s|
  s.name     = "delayed_job"
  s.version  = "1.7.0"
  s.date     = "2008-11-28"
  s.summary  = "Database-backed asynchronous priority queue system -- Extracted from Shopify"
  s.email    = "tobi@leetsoft.com"
  s.homepage = "http://github.com/tobi/delayed_job/tree/master"
  s.description = "Delated_job (or DJ) encapsulates the common pattern of asynchronously executing longer tasks in the background. It is a direct extraction from Shopify where the job table is responsible for a multitude of core tasks."
  s.authors  = ["Tobias Lütke"]

  # s.bindir = "bin"
  # s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  # s.default_executable = "delayed_job"

  s.has_rdoc = false
  s.rdoc_options = ["--main", "README.textile"]
  s.extra_rdoc_files = ["README.textile"]

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.require_paths = ["lib"]
  
  s.add_runtime_dependency(%q<dm-core>,           ["~> 1.0.2"])
  s.add_runtime_dependency(%q<dm-timestamps>,     ["~> 1.0.2"])
  s.add_runtime_dependency(%q<dm-types>,          ["~> 1.0.2"])
end
