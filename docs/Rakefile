# Documentation content to copy into _jekyll for conversion into HTML
$user_files = [
    'images',
    'pages',
    '404.md',
    'index.md'
]

# ---- Rake tasks
desc 'Install dependencies to build project'
task :dependencies do
    # Install build dependencies
    sh 'cd _jekyll;bundle install'
end

task :symlink_assets do
    $user_files.each{ |file| FileUtils.ln_s '../'+file, '_jekyll/'+file }
end

desc 'Clean up generated files'
task :clean do
    $user_files.each{ |file| rm_rf '_jekyll/'+file }
    rm_rf '_jekyll/_site'
    rm_rf '_site'
end

desc 'Generate HTML and build site'
task :build => ['clean', 'symlink_assets'] do
    generate_config()
    jekyll('build')
    FileUtils.ln_s '_jekyll/_site', '_site'
end

task :serve => ['clean', 'build'] do
    jekyll('serve')
    :clean
end

task :test do
  require 'html-proofer'

  jekyll('build')
  options = {
      :assume_extension => true,
      :allow_hash_href => true,
      :url_ignore => [
          # The favicon is not in the repo
          '/favicon.ico',
          # This is the address of engine by default
          %r{.*://localhost:4567.*},
          # We generate links to the pages within our repo which may not exist yet
          %r{.*github\.com/graknlabs/grakn/tree/.*},
          %r{.*github\.com/graknlabs/sample-(projects|datasets).*},
          # These ones mysteriously go wrong even though they were fine when I last checked
          %r{https://javadoc.io/.*},
          'https://docs.docker.com/docker-for-windows/'
      ],
      # avoid SSL errors: https://github.com/gjtorikian/html-proofer/issues/376
      :typhoeus => {
        :ssl_verifypeer => false,
        :ssl_verifyhost => 0
      }
  }
  HTMLProofer.check_directory("./_jekyll/_site", options).run
end

# ---- Rake functions

# Run Jekyll
def jekyll(opts='')
   sh "cd _jekyll; bundle exec jekyll #{opts} --trace"
end

def generate_config()
    text = File.read('_jekyll/_config-template.yml')

    if ENV['urlprefix']
        output = text.gsub(/PREFIX/, ENV['urlprefix'])
    else
        output = text.gsub(/PREFIX/, "")
    end

    File.open('_jekyll/_config.yml', 'w'){ |file| file.puts output }
end

