FROM ruby:latest
RUN mkdir /logicmonitor
COPY ./ logicmonitor
WORKDIR /logicmonitor
RUN bundle install
RUN gem build fluent-plugin-lm-logs-gcp.gemspec
RUN mv fluent-plugin-lm-logs-gcp-*.gem release.gem