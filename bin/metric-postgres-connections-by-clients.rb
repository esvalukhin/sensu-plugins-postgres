#! /usr/bin/env ruby
#
#   metric-postgres-connections-by-clients
#
# DESCRIPTION:
#
#   This plugin collects postgres connection metrics by connected clients
#
# OUTPUT:
#   metric data
#
# PLATFORMS:
#   Linux
#
# DEPENDENCIES:
#   gem: sensu-plugin
#   gem: pg
#
# USAGE:
#   ./metric-postgres-connections.rb -u db_user -p db_pass -h db_host -d db
#
# NOTES:
#
# LICENSE:
#   Copyright (c) 2012 Kwarter, Inc <platforms@kwarter.com>
#   Author Gilles Devaux <gilles.devaux@gmail.com>
#   Released under the same terms as Sensu (the MIT license); see LICENSE
#   for details.
#

require 'sensu-plugin/metric/cli'
require 'pg'
require 'socket'

class PostgresStatsDBMetrics < Sensu::Plugin::Metric::CLI::Graphite
  option :user,
         description: 'Postgres User',
         short: '-u USER',
         long: '--user USER'

  option :password,
         description: 'Postgres Password',
         short: '-p PASS',
         long: '--password PASS'

  option :hostname,
         description: 'Hostname to login to',
         short: '-h HOST',
         long: '--hostname HOST',
         default: 'localhost'

  option :port,
         description: 'Database port',
         short: '-P PORT',
         long: '--port PORT',
         default: 5432

  option :db,
         description: 'Database name',
         short: '-d DB',
         long: '--db DB',
         default: 'postgres'
 
  option :delimeter,
         description: 'Delimeter sign for tags',
         short: '-s DELIMETER',
         long: '--delimiter-sign DELIMETER',
         default: ':'

  option :scheme,
         description: 'Metric naming scheme, text to prepend to $queue_name.$metric',
         long: '--scheme SCHEME',
         default: "#{Socket.gethostname}.postgresql"

  def run
    timestamp = Time.now.to_i

    con     = PG::Connection.new(config[:hostname], config[:port], nil, nil, 'postgres', config[:user], config[:password])
    request = [
      'select count(*), client_addr, waiting from pg_stat_activity',
      "where datname = '#{config[:db]}' group by client_addr, waiting order by client_addr"
    ]

    clients = {}
    
    con.exec(request.join(' ')) do |result|
      result.each do |row|
        client_ip = row['client_addr']
        clients[client_ip] = {} unless clients[client_ip]
        metrics = {
          active: 0,
          waiting: 0,
          total: 0
        }
        if row['waiting'] == 't'
          metrics[:waiting] = row['count']
        elsif row['waiting'] == 'f'
          metrics[:active] = row['count']
        end
        metrics = metrics.merge(clients[client_ip]) { |_key, oldval, newval| newval + oldval }
        clients[client_ip] = metrics
      end
    end

    clients.each do |key,value|      
        value[:total] = (value[:waiting].to_i + value[:active].to_i)
    end

    metric = {:active => 0, :waiting => 0, :total => 0}
    
    clients.each do |key,value|
      metric[:active] += value[:active].to_i
      metric[:waiting] += value[:waiting].to_i
      metric[:total] += value[:total].to_i
    end

    clients[:all] = metric    

    clients.each do |client, metrics|
      metrics.each do |key, value|
          output "#{config[:scheme]}.connections.#{config[:db]}.#{key}#{config[:delimeter]}host=#{client}", value, timestamp
      end
    end

    ok
  end
end
