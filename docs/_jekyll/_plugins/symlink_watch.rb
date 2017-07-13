# Copyright 2014 Google. All rights reserved.
# Available under an MIT license that can be found in the LICENSE file.

# The symlink_watcher plugin extends jekyll-watch to also listen for changes in
# any symlinked sub-directories.
#
# For example, my _drafts directory is a symlink to a directory elsewhere on my
# filesystem.  This plugin will cause jekyll to regenerate my site when any
# files in my drafts folder change.

require "find"
require "jekyll-watch"

module Jekyll
  module Watcher
    def build_listener_with_symlinks(site, options)
      src = options["source"]
      dirs = [src]
      Find.find(src).each do |f|
        next if f == "#{src}/_drafts" and not options["show_drafts"]
        # TODO(willnorris): filter ignored files
        dirs << f if File.directory?(f) and File.symlink?(f)
      end

      require "listen"
      Listen.to(
        *dirs,
        :ignore => listen_ignore_paths(options),
        :force_polling => options['force_polling'],
        &(listen_handler(site))
      )
    end

    alias_method :build_listener_without_symlinks, :build_listener
    alias_method :build_listener, :build_listener_with_symlinks
  end
end

