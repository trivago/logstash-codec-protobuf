# encoding: utf-8
require 'logstash/codecs/base'
require 'logstash/util/charset'
require 'protocol_buffers' # https://github.com/codekitchen/ruby-protocol-buffers

class LogStash::Codecs::Protobuf < LogStash::Codecs::Base
  config_name 'protobuf'

  # Required: list of strings containing directories or files with protobuf definitions
  config :include_path, :validate => :array, :required => true

  # Name of the class to decode
  config :class_name, :validate => :string, :required => true

  # For benchmarking only, not intended for public use: change encoder strategy. 
  # valid method names are:  encoder_strategy_1 (the others are not implemented yet)
  config :encoder_method, :validate => :string, :default => "encoder_strategy_1"

  # Whether to treat data to decode as complete serialized protobuf objects or
  # as a stream of serialized protobuf bytes with each protobuf object
  # prefixed with a varint length
  config :streaming, :validate => :boolean, :default => false

  def register
    @pb_metainfo = {}
    include_path.each { |path| require_pb_path(path) }
    @obj = create_object_from_name(class_name)
    @logger.debug("Protobuf files successfully loaded.")

    if @streaming
      @logger.debug("Activating streaming mode.")
      @buffer = ProtocolBuffers.bin_sio("", mode="w+")
      @proto_length = -1
    end
  end

  def decode(data)
    # If we are streaming, attempt to find a valid varint
    if @streaming
      # Add this new piece of information to the end of the buffer
      @buffer.pos = @buffer.size
      @buffer.write(data)

      # Rewind to beginning to start decoding attempt
      @buffer.rewind

      # While buffer contains all bytes as needed to read the current protobuf
      while @proto_length <= (@buffer.size - @buffer.pos)
        if @proto_length < 0
          # If we don't know the size of the probotuf, we need to find it!
          begin
            @proto_length = get_protobuf_length
            if @logger.debug?
              @logger.debug("Found expected length of next protobuf: #{@proto_length}")
            end
          rescue NotEnoughData
            # Go out of while and wait for another piece of data
            break
          rescue ProtocolBuffers::DecodeError => e
            # If we have enough data but decoding failed, propagate this up to
            # the logstash input to trigger a reconnection if possible as we
            # can't sanely recover on codec side alone.
            @logger.error("Invalid varint detected. Propagating exception to input")
            raise e
          end

          # If we got this far, we set a non-negative value for expected length
          # so lets retry this while loop
        else
          # If we know the size of the protobuf, get its bytes and decode it!
          proto_data = ''
          @buffer.read(@proto_length, proto_data)
          # Reset expected length to -1 since we need to read another varint
          @proto_length = -1
          yield decode_protobuf(proto_data) if block_given?
        end
      end

      # After processing everything we could, remove everything from the buffer
      # that we no longer need (everything between 0 and @buffer.pos)
      compact_buffer
    else
      yield decode_protobuf(data) if block_given?
    end
  end # def decode

  def keys2strings(data)
    if data.is_a?(::Hash)
      new_hash = Hash.new
      data.each{|k,v| new_hash[k.to_s] = keys2strings(v)}
      new_hash
    else
      data
    end
  end

  def encode(event)
    protobytes = generate_protobuf(event)
    @on_event.call(event, protobytes)
  end # def encode

  private
  def decode_protobuf(proto_data)
    proto_data = proto_data.to_s
    if @logger.debug?
      @logger.debug("Decoding protobuf of length #{proto_data.size}")
    end
    decoded = @obj.parse(proto_data)
    results = keys2strings(decoded.to_hash)
    LogStash::Event.new(results)
  end

  def compact_buffer
    if @buffer.pos > 0
      if @logger.debug?
        @logger.debug("Truncating #{@buffer.pos} bytes from beginning of buffer")
      end
      remaining = ''
      @buffer.read(nil, remaining)
      @buffer = ProtocolBuffers.bin_sio(remaining, mode="a+")
    end
  end

  def get_protobuf_length
    starting_pos = @buffer.pos
    begin
      return ProtocolBuffers::Varint.decode(@buffer)
    rescue => e
      # If we weren't able to find a varint with the available data, reset
      # buffer position to the place we started at.
      @buffer.pos = starting_pos
      case e
        when ProtocolBuffers::DecodeError
          raise e
        else
          raise NotEnoughData
      end
    end
  end

  class NotEnoughData < StandardError; end

  def generate_protobuf(event)
    meth = self.method(encoder_method)
    data = meth.call(event, @class_name)
    begin
      msg = @obj.new(data)
      msg.serialize_to_string
    rescue NoMethodError
      @logger.debug("error 2: NoMethodError. Maybe mismatching protobuf definition. Required fields are: " + event.to_hash.keys.join(", "))
    end
  end

  def encoder_strategy_1(event, class_name)
    _encoder_strategy_1(event.to_hash, class_name)

  end

  def _encoder_strategy_1(datahash, class_name)
    fields = clean_hash_keys(datahash)
    fields = flatten_hash_values(fields) # TODO we could merge this and the above method back into one to save one iteration, but how are we going to name it?
    meta = get_complex_types(class_name) # returns a hash with member names and their protobuf class names
    meta.map do | (k,typeinfo) |
      if fields.include?(k)
        original_value = fields[k] 
        proto_obj = create_object_from_name(typeinfo)
        fields[k] = 
          if original_value.is_a?(::Array) 
            ecs1_list_helper(original_value, proto_obj, typeinfo)
            
          else 
            recursive_fix = _encoder_strategy_1(original_value, class_name)
            proto_obj.new(recursive_fix)
          end # if is array
      end

    end 
    
    fields
  end

  def ecs1_list_helper(value, proto_obj, class_name)
    # make this field an array/list of protobuf objects
    # value is a list of hashed complex objects, each of which needs to be protobuffed and
    # put back into the list.
    next unless value.is_a?(::Array)
    value.map { |x| _encoder_strategy_1(x, class_name) } 
    value
  end

  def flatten_hash_values(datahash)
    # 2) convert timestamps and other objects to strings
    next unless datahash.is_a?(::Hash)
    
    ::Hash[datahash.map{|(k,v)| [k, (convert_to_string?(v) ? v.to_s : v)] }]
  end

  def clean_hash_keys(datahash)
    # 1) remove @ signs from keys 
    next unless datahash.is_a?(::Hash)
    
    ::Hash[datahash.map{|(k,v)| [remove_atchar(k.to_s), v] }]
  end #clean_hash_keys

  def convert_to_string?(v)
    !(v.is_a?(Fixnum) || v.is_a?(::Hash) || v.is_a?(::Array) || [true, false].include?(v))
  end

   
  def remove_atchar(key) # necessary for @timestamp fields and the likes. Protobuf definition doesn't handle @ in field names well.
    key.dup.gsub(/@/,'')
  end

  private
  def create_object_from_name(name)
    begin
      @logger.debug("Creating instance of " + name)
      name.split('::').inject(Object) { |n,c| n.const_get c }
     end
  end

  def get_complex_types(class_name)
    @pb_metainfo[class_name]
  end

  def require_with_metadata_analysis(filename)
    require filename
    regex_class_name = /\s*class\s*(?<name>.+?)\s+/
    regex_module_name = /\s*module\s*(?<name>.+?)\s+/
    regex_pbdefs = /\s*(optional|repeated)(\s*):(?<type>.+),(\s*):(?<name>\w+),(\s*)(?<position>\d+)/
    # now we also need to find out which class it contains and the protobuf definitions in it.
    # We'll unfortunately need that later so that we can create nested objects.
    begin 
      class_name = ""
      type = ""
      field_name = ""
      classname_found = false
      File.readlines(filename).each do |line|
        if ! (line =~ regex_module_name).nil? && !classname_found # because it might be declared twice in the file
          class_name << $1 
          class_name << "::"
    
        end
        if ! (line =~ regex_class_name).nil? && !classname_found # because it might be declared twice in the file
          class_name << $1
          @pb_metainfo[class_name] = {}
          classname_found = true
        end
        if ! (line =~ regex_pbdefs).nil?
          type = $1
          field_name = $2
          if type =~ /::/
            @pb_metainfo[class_name][field_name] = type.gsub!(/^:/,"")
            
          end
        end
      end
    rescue Exception => e
      @logger.warn("error 3: unable to read pb definition from file  " + filename+ ". Reason: #{e.inspect}. Last settings were: class #{class_name} field #{field_name} type #{type}. Backtrace: " + e.backtrace.inspect.to_s)
    end
    if class_name.nil?
      @logger.warn("error 4: class name not found in file  " + filename)
    end    
  end

  def require_pb_path(dir_or_file)
    f = dir_or_file.end_with? ('.rb')
    begin
      if f
        @logger.debug("Including protobuf file: " + dir_or_file)
        require_with_metadata_analysis dir_or_file
      else 
        Dir[ dir_or_file + '/*.rb'].each { |file|
          @logger.debug("Including protobuf path: " + dir_or_file + "/" + file)
          require_with_metadata_analysis file 
        }
      end
    end
  end


end # class LogStash::Codecs::Protobuf
