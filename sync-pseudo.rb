#!/usr/bin/env ruby

Commit = Struct.new(:op, :ndx, :value, :peer, :timestamp, :version, :peer_version, :peer_ndx)

$timestamp = 0

class TransformIndexMap
    def initialize
        @map = []
    end

    def add_commit(commit)
        return unless commit.op == :insert
        if commit.peer_ndx.nil?
            @map << [commit.ndx, 0]
            @map.each do |pair|
                if pair[0] >= commit.ndx
                    pair[1] += 1
                end
            end
        else
            # Peer inserts are incorporated as 
            tndx = transform(commit.peer_ndx)
            @map << [tndx, 0]
            @map.map! { |pair|
                ndx = pair[0] > tndx ? pair[0] + 1 : pair[0]
                [ndx, pair[1]]
            }
        end
        sort_and_collapse!
    end

    def transform(ndx)
        transformation = @map.reverse.detect { |pair| ndx >= pair[0] }
        transformation ? ndx + transformation[1] : ndx
    end

    def sort_and_collapse!
        @map.sort_by! { |pair| pair[0] }
        @map = @map.group_by { |pair| pair[0] }
        @map = @map.map { |x| [x[0], x[1].map {|pair| pair[1]}.inject(0, &:+)] }
        # Merge adjacent indices
        last_seen_ndx = nil
        result = []
        @map.each do |pair|
            if last_seen_ndx && pair[0] == last_seen_ndx + 1
                result.last[1] += pair[1]
            else
                result << pair
            end
            last_seen_ndx = pair[0]
        end
        @map = result
    end
end

class Peer
    def initialize(id)
        @id = id
        @list = []
        @commit_log = []
        @version = 1
    end
    attr_reader :id
    attr_reader :list

    def last_peer_version
        @commit_log.empty? ? 0 : @commit_log.last.peer_version
    end

    def insert(ndx, value, timestamp = nil)
        timestamp ||= ($timestamp += 1)
        @list.insert(ndx, value)
        @version += 1
        @commit_log << Commit.new(:insert, ndx, value, @id, timestamp, @version, last_peer_version, nil)
        puts "* (#{@id}) INSERT: #{@commit_log.last}"
        #puts "(#{@id}) commit log: #{@commit_log.inspect}"
    end

    def set(ndx, value, timestamp = nil)
        timestamp ||= ($timestamp += 1)
        @list[ndx] = value
        @version += 1
        @commit_log << Commit.new(:set, ndx, value, @id, timestamp, @version, last_peer_version, nil)
        puts "* (#{@id}) SET: #{@commit_log.last}"
    end

    def commits_from_version(v0)
        @commit_log.select { |c| c.version > v0 }
    end

    def sync_from(peer)
        peer.commits_from_version(last_peer_version).each do |commit|
            next if commit.peer == @id
            sync_one(peer, commit)
        end
    end

    def transform_index_map(v0, v1)
        commits = @commit_log.select { |c| c.version > v0 && c.version <= v1 }

        puts "CREATING MAP FROM COMMITS:"
        commits.each do |c|
            puts "  #{c}"
        end
        result = TransformIndexMap.new
        commits.each do |c|
            result.add_commit(c)
        end
        puts "RESULT = #{result.inspect}"
        result
    end

    def sync_one(peer, commit)
        puts "SYNC: #{peer.id} -> #{@id}  => #{commit}"

        # Generate a map of what happened between the last version that the incoming commit saw,
        # and the last commit with a lower timestamp.
        v0 = commit.peer_version
        v1_commit = @commit_log.reverse.detect { |c| c.timestamp < commit.timestamp }
        if v1_commit
            v1 = v1_commit.version
        else
            v1 = 0
        end

        puts "(#{@id}) Translate Map v#{v0} -> v#{v1}"
        map = transform_index_map(v0, v1)
        ndx = map.transform(commit.ndx)
        puts "(#{@id}) Translate Index #{commit.ndx} -> #{ndx}"
        if commit.op == :insert
            @list.insert(ndx, commit.value)
        elsif commit.op == :set
            @list[ndx] = commit.value
        end
        timestamp = ($timestamp += 1)
        @version += 1
        @commit_log << Commit.new(commit.op, ndx, commit.value, commit.peer, timestamp, @version, commit.version, commit.ndx)
    end
end

server = Peer.new("server")
client = Peer.new("client")

server.insert(0, 100)
server.insert(1, 101)

client.insert(0, 200)

server.insert(2, 102)
server.insert(3, 103)

puts "server: " + server.list.inspect
puts "client: " + client.list.inspect

server.sync_from(client)

puts "server: " + server.list.inspect
puts "client: " + client.list.inspect

client.sync_from(server)

puts "server: " + server.list.inspect
puts "client: " + client.list.inspect

client.insert(3, 400)
server.insert(3, 500)
client.set(3, 401)

client.sync_from(server)
server.sync_from(client)

puts "client: " + client.list.inspect
puts "server: " + server.list.inspect
