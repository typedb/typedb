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

