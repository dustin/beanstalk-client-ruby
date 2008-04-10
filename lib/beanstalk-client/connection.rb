# beanstalk-client/connection.rb - client library for beanstalk

# Copyright (C) 2007 Philotic Inc.

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

require 'socket'
require 'fcntl'
require 'yaml'
require 'set'
require 'beanstalk-client/errors'
require 'beanstalk-client/job'

module Beanstalk
  class Connection
    attr_reader :addr

    def initialize(addr, jptr=self)
      @addr = addr
      @jptr = jptr
      connect
      @last_used = 'default'
      @watch_list = ['default']
    end

    def connect
      host, port = addr.split(':')
      @socket = TCPSocket.new(host, port.to_i)

      # Don't leak fds when we exec.
      @socket.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
    end

    def close
      @socket.close
      @socket = nil
    end

    def put(body, pri=65536, delay=0, ttr=120)
      interact("put #{pri} #{delay} #{ttr} #{body.size}\r\n#{body}\r\n",
               %w(INSERTED BURIED))[0].to_i
    end

    def yput(obj, pri=65536, delay=0, ttr=120)
      put(YAML.dump(obj), pri, delay, ttr)
    end

    def peek()
      interact("peek\r\n", :job)
    rescue UnexpectedResponse
      nil
    end

    def peek_job(id)
      interact("peek #{id}\r\n", :job)
    rescue UnexpectedResponse
      nil
    end

    def reserve()
      raise WaitingForJobError if @waiting
      @socket.write("reserve\r\n")

      begin
        @waiting = true
        # Give the user a chance to select on multiple fds.
        Beanstalk.select.call([@socket]) if Beanstalk.select
      rescue WaitingForJobError
        # just continue
      ensure
        @waiting = false
      end

      Job.new(@jptr, *read_job('RESERVED'))
    end

    def delete(id)
      interact("delete #{id}\r\n", %w(DELETED))
      :ok
    end

    def release(id, pri, delay)
      interact("release #{id} #{pri} #{delay}\r\n", %w(RELEASED))
      :ok
    end

    def bury(id, pri)
      interact("bury #{id} #{pri}\r\n", %w(BURIED))
      :ok
    end

    def use(tube)
      return tube if tube == @last_used
      @last_used = interact("use #{tube}\r\n", %w(USING))[0]
    end

    def watch(tube)
      return @watch_list.size if @watch_list.include?(tube)
      r = interact("watch #{tube}\r\n", %w(WATCHING))[0].to_i
      @watch_list += [tube]
      return r
    end

    def ignore(tube)
      return @watch_list.size if !@watch_list.include?(tube)
      r = interact("ignore #{tube}\r\n", %w(WATCHING))[0].to_i
      @watch_list -= [tube]
      return r
    end

    def stats()
      interact("stats\r\n", :yaml)
    end

    def job_stats(id)
      interact("stats-job #{id}\r\n", :yaml)
    end

    def stats_tube(tube)
      interact("stats-tube #{tube}\r\n", :yaml)
    end

    def list_tubes()
      interact("list-tubes\r\n", :yaml)
    end

    def list_tube_used()
      interact("list-tube-used\r\n", %w(USING))[0]
    end

    def list_tubes_watched(cached=false)
      return @watch_list if cached
      @watch_list = interact("list-tubes-watched\r\n", :yaml)
    end

    private

    def interact(cmd, rfmt)
      raise WaitingForJobError if @waiting
      @socket.write(cmd)
      return read_yaml('OK') if rfmt == :yaml
      return Job.new(@jptr, *read_job('FOUND')) if rfmt == :job
      check_resp(*rfmt)
    end

    def get_resp()
      r = @socket.gets("\r\n")
      raise EOFError if r == nil
      r[0...-2]
    end

    def check_resp(*words)
      r = get_resp()
      rword, *vals = r.split(/\s+/)
      if (words.size > 0) and !words.include?(rword)
        raise UnexpectedResponse.classify(rword, r)
      end
      vals
    end

    def read_job(word)
      id, bytes = check_resp(word).map{|s| s.to_i}
      body = read_bytes(bytes)
      raise 'bad trailer' if read_bytes(2) != "\r\n"
      [id, body]
    end

    def read_yaml(word)
      bytes_s, = check_resp(word)
      yaml = read_bytes(bytes_s.to_i)
      raise 'bad trailer' if read_bytes(2) != "\r\n"
      YAML::load(yaml)
    end

    def read_bytes(n)
      str = @socket.read(n)
      raise EOFError, 'End of file reached' if str == nil
      raise EOFError, 'End of file reached' if str.size < n
      str
    end
  end

  class CleanupWrapper
    def initialize(addr, multi)
      @conn = Connection.new(addr, self)
      @multi = multi
    end

    def method_missing(selector, *args, &block)
      begin
        @conn.send(selector, *args, &block)
      rescue EOFError, Errno::ECONNRESET, Errno::EPIPE, UnexpectedResponse => ex
        @multi.remove(@conn)
        raise ex
      end
    end
  end

  class Pool
    def initialize(addrs)
      @addrs = addrs
      connect()
    end

    def connect()
      @connections ||= {}
      @addrs.each do |addr|
        begin
          if !@connections.include?(addr)
            puts "connecting to beanstalk at #{addr}"
            @connections[addr] = CleanupWrapper.new(addr, self)
          end
        rescue Exception => ex
          puts "#{ex.class}: #{ex}"
          #puts begin ex.fixed_backtrace rescue ex.backtrace end
        end
      end
      @connections.size
    end

    def open_connections()
      @connections.values()
    end

    def last_server
      @last_conn.addr
    end

    def put(body, pri=65536, delay=0, ttr=120)
      send_to_rand_conn(:put, body, pri, delay, ttr)
    end

    def yput(obj, pri=65536, delay=0, ttr=120)
      send_to_rand_conn(:yput, obj, pri, delay, ttr)
    end

    def reserve()
      send_to_rand_conn(:reserve)
    end

    def use(tube)
      send_to_all_conns(:use, tube)
    end

    def watch(tube)
      send_to_all_conns(:watch, tube)
    end

    def ignore(tube)
      send_to_all_conns(:ignore, tube)
    end

    def raw_stats()
      send_to_all_conns(:stats)
    end

    def stats()
      sum_hashes(raw_stats.values)
    end

    def raw_stats_tube(tube)
      send_to_all_conns(:stats_tube, tube)
    end

    def stats_tube(tube)
      sum_hashes(raw_stats_tube(tube).values)
    end

    def list_tubes()
      send_to_all_conns(:list_tubes)
    end

    def list_tube_used()
      send_to_all_conns(:list_tube_used)
    end

    def list_tubes_watched()
      send_to_all_conns(:list_tubes_watched)
    end

    def remove(conn)
      @connections.delete(conn.addr)
    end

    def close
      while @connections.size > 0
        addr = @connections.keys.last
        conn = @connections[addr]
        @connections.delete(addr)
        conn.close
      end
    end

    def peek()
      open_connections.each do |c|
        job = c.peek
        return job if job
      end
      nil
    end

    def peek_job(id)
      make_hash(send_to_all_conns(:peek_job, id))
    end

    private

    def send_to_rand_conn(sel, *args)
      wrap(pick_connection, sel, *args)
    end

    def send_to_all_conns(sel, *args)
      compact_hash(make_hash(@connections.map{|a, c| [a, wrap(c, sel, *args)]}))
    end

    def pick_connection()
      open_connections[rand(open_connections.size)] or raise NotConnected
    end

    def wrap(conn, sel, *args)
      (@last_conn = conn).send(sel, *args)
    rescue DrainingError
      # Don't reconnect -- we're not interested in this server
      retry
    rescue EOFError, Errno::ECONNRESET, Errno::EPIPE
      connect()
      retry
    end

    def make_hash(pairs)
      Hash[*pairs.inject([]){|a,b|a+b}]
    end

    def compact_hash(hash)
      hash.reject{|k,v| v == nil}
    end

    def sum_hashes(hs)
      hs.inject({}){|a,b| a.merge(b) {|k,o,n| combine_stats(k, o, n)}}
    end

    DONT_ADD = Set['name', 'version', 'pid']
    def combine_stats(k, a, b)
      DONT_ADD.include?(k) ? Set[a] + Set[b] : a + b
    end
  end
end
