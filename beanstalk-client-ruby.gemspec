Gem::Specification.new do |s|
  s.name = %q{beanstalk-client}
  s.version = "0.11.2"

  s.specification_version = 2 if s.respond_to? :specification_version=

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Keith Rarick"]
  s.date = %q{2008-05-01}
  s.description = %q{Ruby client library for the Beanstalk protocol}
  s.email = %q{kr@essembly.com}
  s.extra_rdoc_files = ["History.txt", "License.txt", "Manifest.txt", "README.txt", "website/index.txt"]
  s.files = ["History.txt", "License.txt", "Manifest.txt", "README.txt", "Rakefile", "config/hoe.rb", "config/requirements.rb", "lib/beanstalk-client.rb", "lib/beanstalk-client/connection.rb", "lib/beanstalk-client/errors.rb", "lib/beanstalk-client/job.rb", "lib/beanstalk-client/version.rb", "log/debug.log", "script/destroy", "script/generate", "script/txt2html", "setup.rb", "tasks/deployment.rake", "tasks/environment.rake", "tasks/website.rake", "test/test_beanstalk-client.rb", "test/test_helper.rb", "website/index.html", "website/index.txt", "website/javascripts/rounded_corners_lite.inc.js", "website/stylesheets/screen.css", "website/template.rhtml"]
  s.has_rdoc = true
  s.homepage = %q{http://beanstalk.rubyforge.org}
  s.rdoc_options = ["--main", "README.txt"]
  s.require_paths = ["lib"]
  s.rubyforge_project = %q{beanstalk}
  s.rubygems_version = %q{1.0.1}
  s.summary = %q{Ruby client library for the Beanstalk protocol}
  s.test_files = ["test/test_beanstalk-client.rb", "test/test_helper.rb"]
end
