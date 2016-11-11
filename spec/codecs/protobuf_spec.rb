# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/protobuf"
require "logstash/event"
require 'protocol_buffers' # https://github.com/codekitchen/ruby-protocol-buffers
require "insist"

describe LogStash::Codecs::Protobuf do


  context "#decode" do


    #### Test case 1: Decode simple protobuf bytes for unicorn ####################################################################################################################
    let(:plugin_unicorn) { LogStash::Codecs::Protobuf.new("class_name" => "Animal::Unicorn", "include_path" => ['spec/helpers/unicorn.pb.rb'])  }
    before do
        plugin_unicorn.register      
    end

    it "should return an event from protobuf encoded data" do
    
      data = {:colour => 'rainbow', :horn_length => 18, :last_seen => 1420081471, :has_wings => true}
      unicorn = Animal::Unicorn.new(data)
        
      plugin_unicorn.decode(unicorn.serialize_to_string) do |event|
        expect(event["colour"] ).to eq(data[:colour] )
        expect(event["horn_length"] ).to eq(data[:horn_length] )
        expect(event["last_seen"] ).to eq(data[:last_seen] )
        expect(event["has_wings"] ).to eq(data[:has_wings] )
      end
    end # it



    #### Test case 2: Decode complex protobuf bytes for human #####################################################################################################################


  
 
    let(:plugin_human) { LogStash::Codecs::Protobuf.new("class_name" => "Animal::Human", "include_path" => ['spec/helpers/human.pb.rb'])  }
    before do
        plugin_human.register      
    end

    it "should return an event from complex nested protobuf encoded data" do
    
      data_gm = {:first_name => 'Elisabeth', :last_name => "Oliveoil", :middle_names => ["Maria","Johanna"], :vegetarian=>true}
      grandmother = Animal::Human.new(data_gm)
      data_m = {:first_name => 'Annemarie', :last_name => "Smørebrød", :mother => grandmother}
      mother = Animal::Human.new(data_m)
      data_f = {:first_name => 'Karl', :middle_names => ["Theodor-Augustin"], :last_name => "Falkenstein"}
      father = Animal::Human.new(data_f)
      data = {:first_name => 'Hugo', :middle_names => ["Heinz", "Peter"], :last_name => "Smørebrød",:father => father, :mother => mother}  
      hugo = Animal::Human.new(data)

      num_events = 0
       
      plugin_human.decode(hugo.serialize_to_string) do |event|
        num_events += 1

        expect(event["first_name"] ).to eq(data[:first_name] )
        expect(event["middle_names"] ).to eq(data[:middle_names] )
        expect(event["last_name"] ).to eq(data[:last_name] )
        expect(event["mother"]["first_name"] ).to eq(data_m[:first_name] ) 
        expect(event["father"]["first_name"] ).to eq(data_f[:first_name] )
        expect(event["mother"]["last_name"] ).to eq(data_m[:last_name] )
        expect(event["mother"]["mother"]["last_name"] ).to eq(data_gm[:last_name] )
        expect(event["mother"]["mother"]["first_name"] ).to eq(data_gm[:first_name] )
        expect(event["mother"]["mother"]["middle_names"] ).to eq(data_gm[:middle_names] )
        expect(event["mother"]["mother"]["vegetarian"] ).to eq(data_gm[:vegetarian] )
        expect(event["father"]["last_name"] ).to eq(data_f[:last_name] )
        expect(event["father"]["middle_names"] ).to eq(data_f[:middle_names] )
      end

      expect(num_events).to eq(1)
    end # it






    #### Test case 3: Decoder test for enums #####################################################################################################################


  
 
    let(:plugin_col) { LogStash::Codecs::Protobuf.new("class_name" => "ColourProtoTest", "include_path" => ['spec/helpers/ColourTestcase.pb.rb'])  }
    before do
        plugin_col.register      
    end

    it "should return an event from protobuf encoded data with enums" do
    
      data = {:least_liked => ColourProtoTest::Colour::YELLOW, :favourite_colours => \
        [ColourProtoTest::Colour::BLACK, ColourProtoTest::Colour::BLUE], :booleantest => [true, false, true]}  
      pb = ColourProtoTest.new(data)

      num_events = 0

      plugin_col.decode(pb.serialize_to_string) do |event|
        num_events += 1

        expect(event["least_liked"] ).to eq(data[:least_liked] )
        expect(event["favourite_colours"] ).to eq(data[:favourite_colours] )
        expect(event["booleantest"] ).to eq(data[:booleantest] )
      end

      expect(num_events).to eq(1)
    end # it
  end # context

  context "#streaming-decode" do

    def protobufs_to_string(protobufs)
      buffer = ProtocolBuffers.bin_sio("", mode="w+")

      protobufs.each do |protobuf|
        serialized_protobuf = protobuf.serialize_to_string
        ProtocolBuffers::Varint.encode(buffer, serialized_protobuf.size)
        buffer << serialized_protobuf
      end

      return buffer.string
    end

    let(:plugin_unicorn) {
      LogStash::Codecs::Protobuf.new(
          "class_name" => "Animal::Unicorn",
          "include_path" => ['spec/helpers/unicorn.pb.rb'],
          "streaming" => true)
    }
    before do
      plugin_unicorn.register
    end

    #### Test case 4: Streaming decoding (single entire object) ################

    it "should return an event from a single protobuf encoded data" do

      data = {:colour => 'rainbow', :horn_length => 18, :last_seen => 1420081471, :has_wings => true}
      unicorn = Animal::Unicorn.new(data)

      num_events = 0

      plugin_unicorn.decode(protobufs_to_string([unicorn])) do |event|
        num_events += 1
        expect(event["colour"] ).to eq(data[:colour] )
        expect(event["horn_length"] ).to eq(data[:horn_length] )
        expect(event["last_seen"] ).to eq(data[:last_seen] )
        expect(event["has_wings"] ).to eq(data[:has_wings] )
      end

      expect(num_events).to eq(1)
    end # it

    #### Test case 5: Streaming decoding (multiple entire objects) #############

    it "should return 2 events from protobuf encoded data containing" do

      data = [
          {:colour => 'rainbow', :horn_length => 18, :last_seen => 1420081471, :has_wings => true},
          {:colour => 'invisible', :horn_length => 14, :last_seen => 1420081771, :has_wings => false}
      ]

      unicorns = data.map { |data_item| Animal::Unicorn.new(data_item) }

      plugin_unicorn.decode(protobufs_to_string(unicorns)) do |event|
        expected_data = data.shift

        expect(event["colour"] ).to eq(expected_data[:colour] )
        expect(event["horn_length"] ).to eq(expected_data[:horn_length] )
        expect(event["last_seen"] ).to eq(expected_data[:last_seen] )
        expect(event["has_wings"] ).to eq(expected_data[:has_wings] )
      end

      # Ensure there were actually 2 events
      expect(data.size).to eq(0)
    end # it

    #### Test case 6: Streaming decoding (single object split over 2 decodes) ##

    it "should return an event from a single protobuf encoded data split in 2" do

      data = {:colour => 'rainbow', :horn_length => 18, :last_seen => 1420081471, :has_wings => true}
      unicorn = Animal::Unicorn.new(data)

      serialized_unicorn = protobufs_to_string([unicorn])

      serialized_unicorn_part1 = serialized_unicorn[0, serialized_unicorn.size / 2]
      serialized_unicorn_part2 = serialized_unicorn[serialized_unicorn.size / 2, serialized_unicorn.size]

      num_events = 0

      expect { |b| plugin_unicorn.decode(serialized_unicorn_part1, &b) }.not_to yield_control

      plugin_unicorn.decode(serialized_unicorn_part2) do |event|
        num_events += 1
        expect(event["colour"] ).to eq(data[:colour] )
        expect(event["horn_length"] ).to eq(data[:horn_length] )
        expect(event["last_seen"] ).to eq(data[:last_seen] )
        expect(event["has_wings"] ).to eq(data[:has_wings] )
      end

      expect(num_events).to eq(1)
    end # it

    #### Test case 7: Skip garbage ##

    it "garbage should trigger exception out of codec" do

      # This should make the varint decoding fail
      garbage = 0b1000_0000.chr * 10

      expect { |b| plugin_unicorn.decode(garbage, &b) }.to raise_error(ProtocolBuffers::DecodeError)
    end # it


  end # context





  #### Test case 8: Encode simple protobuf bytes for unicorn ####################################################################################################################

  context "#encode" do
    subject do
      next LogStash::Codecs::Protobuf.new("class_name" => "Animal::UnicornEvent", "include_path" => ['spec/helpers/unicorn_event.pb.rb']) 
    end

    event = LogStash::Event.new("colour" => "pink", "horn_length" => 12, "last_seen" => 1410081999, "has_wings" => true)    

    it "should return protobuf encoded data from a simple event" do
      subject.on_event do |event, data|
        insist { data.is_a? String }
        unicorn = Animal::UnicornEvent.parse(data) 
    
        expect(unicorn.colour ).to eq(event["colour"] )
        expect(unicorn.horn_length ).to eq(event["horn_length"] )
        expect(unicorn.last_seen ).to eq(event["last_seen"] )
        expect(unicorn.has_wings ).to eq(event["has_wings"] )
      
      end # subject.on_event
      subject.encode(event)
    end # it
  end # context




  #### Test case 9: encode complex protobuf bytes for human #####################################################################################################################
  
  
  context "#encode2" do
    subject do
      next LogStash::Codecs::Protobuf.new("class_name" => "Animal::Human", "include_path" => ['spec/helpers/human.pb.rb']) 
    end

    event = LogStash::Event.new("first_name" => "Jimmy", "middle_names" => ["Bob", "James"], "last_name" => "Doe" \
      , "mother" => {"first_name" => "Jane", "middle_names" => ["Elizabeth"], "last_name" => "Doe" , "age" => 83, "vegetarian"=> false} \
      , "father" => {"first_name" => "John", "last_name" => "Doe", "@email" => "character_replacement_test@nothing" })    

    it "should return protobuf encoded data from a complex event" do

      subject.on_event do |event, data|
        insist { data.is_a? String }
        jimmy = Animal::Human.parse(data) 
        
        expect(jimmy.first_name ).to eq(event["first_name"] )
        expect(jimmy.middle_names ).to eq(event["middle_names"] )
        expect(jimmy.last_name ).to eq(event["last_name"] )
        expect(jimmy.mother.first_name ).to eq(event["mother"]["first_name"] )
        expect(jimmy.father.first_name ).to eq(event["father"]["first_name"] )
        expect(jimmy.mother.middle_names ).to eq(event["mother"]["middle_names"] )
        expect(jimmy.mother.age ).to eq(event["mother"]["age"] ) # recursion test for values
        expect(jimmy.mother.vegetarian ).to eq(event["mother"]["vegetarian"] ) # recursion test for values
        expect(jimmy.father.last_name ).to eq(event["father"]["last_name"] )
        expect(jimmy.father.email ).to eq(event["father"]["@email"] ) # recursion test for keys
        expect(jimmy.mother.last_name ).to eq(event["mother"]["last_name"] )
      
      end # subject.on_event
      subject.encode(event)
    end # it
  end # context





  #### Test case 10: encode enums #########################################################################################################################
  

 
  context "#encode3" do
    subject do
      next LogStash::Codecs::Protobuf.new("class_name" => "ColourProtoTest", "include_path" => ['spec/helpers/ColourTestcase.pb.rb'])
    end

    require 'spec/helpers/ColourTestcase.pb.rb' # otherwise we cant use the colour enums in the next line
    event = LogStash::Event.new("booleantest" =>  [false, false, true], "least_liked" => ColourProtoTest::Colour::YELLOW,  "favourite_colours" => \
       [ColourProtoTest::Colour::BLACK, ColourProtoTest::Colour::BLUE] )    

    it "should return protobuf encoded data from a complex event with enums" do

      subject.on_event do |event, data|
        insist { data.is_a? String }

        colpref = ColourProtoTest.parse(data) 
        
        expect(colpref.booleantest ).to eq(event["booleantest"] )
        expect(colpref.least_liked ).to eq(event["least_liked"] )
        expect(colpref.favourite_colours ).to eq(event["favourite_colours"] )

      
      end # subject.on_event
      subject.encode(event)
    end # it
  end # context



end
