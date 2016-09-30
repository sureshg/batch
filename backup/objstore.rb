#!/usr/bin/env ruby

require 'rubygems'
require 'fog'
require 'optparse'

# Util method to display error and option banner.
def exit_with_error(msg, ex = nil, banner = true)
  puts "#{msg} \n"
  puts ex.backtrace if !ex.nil? && @options['verbose']
  puts "\n#{@opt_parser.to_s}" if banner
  exit -1
end


# Command line arg parser.
@cmds = %w(list upload download delete)

def parse (args)
  @options = {}
  @opt_parser = OptionParser.new do |opts|
    opts.banner = "Usage: objstore [-v] [-h] [-p (OpenStack|AWS|Google)] [-o (Key=Value)] [-c (#{@cmds.join('|')})] [args...]"

    @options['verbose'] = false
    opts.on('-v', '--verbose', 'More debugging info') do
      @options['verbose'] = true
    end

    opts.on('-h', '--help', 'Display Help') do
      puts opts
      exit 0
    end

    @options['provider'] = 'openstack'
    opts.on('-p', '--provider name', 'Cloud provider. Defaults to OpenStack(Swift)') do |p|
      @options['provider'] = p.downcase
    end

    opts.on('-o', '--options provider options', 'Cloud provider options, such as credentials. Use Key=Value format') do |o|
      (key, val) = o.split('=')
      @options[key] = val
    end

    @options['cmd'] = ''
    opts.on('-c', '--command name', "Object store commands (#{@cmds.join('|')})") do |c|
      @options['cmd'] = c.downcase
    end
  end

  # Parse the args
  @opt_parser.parse! args

  # Remaining args after parsing
  @options['args'] = args

  # Validations after parsing
  unless %w(openstack).include?(@options['provider'])
    exit_with_error("Cloud Provider #{@options['provider']} is not supported now!")
  end

  if @options['cmd'].nil? || !@cmds.include?(@options['cmd'])
    exit_with_error("Invalid command - #{@options['cmd']}")
  end

  # Process command options.
  case @options['cmd']
    when 'upload'
      exit_with_error('Usage: objstore -c upload [file_name] [bucket_name]') if args.size < 2
    else
      exit_with_error("#{@options['cmd']} command is not supported now!")
  end
end


# Read the env prop file. Used to read /etc/openrc
# and oneops conf file (/etc/profile.d/oneops.sh).
#
# @return map of env key value pairs
def read_prop_file(prop_file)
  props = {}
  File.open(prop_file, 'r') do |f|
    f.each_line do |line|
      props[$1] = $2 if line =~ /^export (.*)=(.*)$/
    end
  end if File.exists?(prop_file)
  props
end


# Returns cloud provider connection params
#
# @return map of connection params for the specific cloud provider.
def get_conn_params

  # Excon.defaults[:read_timeout] = 10
  # Excon.defaults[:write_timeout] = 10
  # Excon.defaults[:ssl_verify_peer] = false

  params = {}
  open_rc = read_prop_file('/etc/openrc')
  provider = @options['provider']
  case provider
    when 'openstack'
      params[:provider] = provider
      params[:openstack_username] = @options['OS_USERNAME'] || open_rc['OS_USERNAME'] || ENV['OS_USERNAME']
      params[:openstack_api_key] = @options['OS_PASSWORD'] || open_rc['OS_PASSWORD'] || ENV['OS_PASSWORD']
      params[:openstack_auth_url] = (@options['OS_AUTH_URL'] || open_rc['OS_AUTH_URL'] || ENV['OS_AUTH_URL']).to_s + '/tokens'
    else
      exit_with_error "Cloud provider: #{provider} not supported!"
  end
  puts "Cloud connection params: #{params}" if @options['verbose']
  params
end

# Pretty format size.
#
# @param size size in bytes.
def format_size(size)
  conv = %w(B KB MB GB TB PB EB)
  scale = 1024
  ndx=1
  if size < 2*(scale**ndx)
    return "#{(size)}#{conv[ndx-1]}"
  end
  size=size.to_f
  [2, 3, 4, 5, 6, 7].each { |ndx|
    if size < 2*(scale**ndx)
      return "#{'%.3f' % (size/(scale**(ndx-1)))}#{conv[ndx-1]}"
    end
  }
  ndx=7
  "#{'%.3f' % (size/(scale**(ndx-1)))}#{conv[ndx-1]}"
end

# Storage upload.
#
# @param args - upload arguments file_name, bucket_name.
#               file_name   :  Local file/dir name to upload
#               bucket_name :  Remote bucket name
def upload(args)
  (file_name, bucket_name) = args
  exit_with_error("Given file/dir not exists, #{file_name}", ex = nil, banner = false) unless File.exists? file_name

  dir = @storage.directories.get bucket_name
  if dir.nil?
    puts "Creating the bucket: #{bucket_name}"
    dir = @storage.directories.create(:key => bucket_name, :public => true)
  end

  puts "Uploading local file/dir: #{file_name} to bucket: #{bucket_name}"
  files = if File.directory?(file_name)
            Dir.chdir(file_name); Dir.glob('**/*')
          else
            [file_name]
          end

  total = files.size
  start = Time.now
  size = 0
  files.each_with_index { |file, idx|
    puts "#{idx+1}/#{total}) Upload: #{file} to #{bucket_name}"
    dir.files.create(
        :key => file,
        :body => File.open(file),
        :public => true
    )
    size += File.size(file)
  }
  finish = Time.now
  puts "\nUpload is completed, total size: #{format_size(size)} took #{finish-start} sec."
end


begin
  #Parse CLI args.
  parse(ARGV)

  #Getting cloud provider connection params.
  cloud_params = get_conn_params
  puts 'Getting the storage connection...'

  #Getting storage connection.
  @storage = Fog::Storage.new(cloud_params)

  #Call command function with args.
  send(@options['cmd'], @options['args'])

rescue => e
  exit_with_error("\nSome error occurred!! #{e.message}", ex = e, banner = false)
end



