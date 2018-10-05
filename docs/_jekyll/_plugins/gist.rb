require 'cgi'
require 'open-uri'

module Fiftyfive
  module Liquid
    class GistTag < ::Liquid::Tag
      def initialize(tag_name, gist_ref, tokens)
        @gist_ref = gist_ref.strip

        # gist_ref is in the form of `gist_id` or `gist_id/filename`
        @gist_id, @filename = @gist_ref.split('/')
        super
      end

      def render(context)
        raw_uri = "https://gist.github.com/raw/#{@gist_id}/#{@filename}"
        script_uri = "https://gist.github.com/#{@gist_id}.js?file=#{@filename}"

        <<MARKUP.strip
<div id="gist-#{@gist_ref.gsub(/[^a-z0-9]/i,'-')}">
<script src="#{script_uri}"></script>
<noscript>
<pre>#{CGI.escapeHTML(open(raw_uri).read.chomp)}</pre>
</noscript>
</div>
MARKUP
      end
    end
  end
end

Liquid::Template.register_tag('gist', Fiftyfive::Liquid::GistTag)
