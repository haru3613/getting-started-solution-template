local cloud2murano = {}
-- This module authenticates the 3rd party cloud callback requests
-- To be updated depending on the security requirements

local configIO = require("vendor.configIO")
local transform = require("vendor.c2c.transform")
local mcrypto = require("staging.mcrypto")
local utils = require("c2c.utils")
local c = require("c2c.vmcache")
local device2 = murano.services.device2 -- to bypass the proxy (device2.lua)
-- Beware of not creating recursive reference with murano2cloud


function findSameTopicInTable(table, candidate)
  if table ~= nil then
    for k, v in pairs(table) do
      if c.AddressMatchWith(candidate, k) then
        return k
      end
    end
  end
  return nil
end

function findplus(address)
  return address:match'^.*()+'
end
-- Propagate event to Murano applications
function cloud2murano.trigger(identity, event_type, payload, options)
    local event = {
      ip = options.ip,
      type = event_type,
      identity = identity,
      timestamp = utils.getTimestamp(options.timestamp),
      connection_id = options.request_id or context.tracking_id,
      payload = payload
    }

    if handle_device2_event then
      -- Triggers the Device2 event handler so the flow is the same as data coming from device2
      return handle_device2_event(event)
    end
end

function cloud2murano.provisioned(identity, data, options)
  -- A new device needs to be created
  if not options then options = {} end
  local key = mcrypto.b64url_encode(mcrypto.rand_bytes(20))
  local result = device2.addIdentity({ identity = identity, auth = { key = key, type = "password" } })
  if result and result.error then return result end

  -- Set configIO default value
  local config_io
  if configIO and configIO.set_to_device then
    config_io = configIO.config_io
  else
    config_io = "<<Config IO is defined globally in the module `vendor.configIO`.>>"
  end
  device2.setIdentityState({ identity = identity, config_io = config_io })

  if result and result.status == 204 then
    -- force to update data at first connection
    device2.setIdentityState(data)
    return cloud2murano.trigger(identity, "provisioned", nil, options)
  end
end

function cloud2murano.deleted(identity, data, options)
  local result = device2.removeIdentity({ identity = identity })
  if result.error then return result end

  return cloud2murano.trigger(identity, "deleted", nil, options)
end

-- This is function handle device data from the 3rd party
-- Also called for murano2cloud module
function cloud2murano.data_in(identity, data, options)
  if type(data) ~= "table" then return end
  if not options then options = {} end
  for k, v in pairs(data) do
    local t = type(v)
    -- Important need to be string if object, the value will be discarded
    if t ~= "string" and t ~= "number" and t ~= "boolean" then data[k] = to_json(v) end
  end

  local result = device2.setIdentityState(data)

  if result and result.status == 404 then
    -- Auto register device on data_in
    result = cloud2murano.provisioned(identity, data, options)
    if result and result.error then return result end
    result = device2.setIdentityState(data)
  end
  if result and result.error then return result end
  if options.notrigger then return result end
  data.identity = nil
  local payload = {{
    values = data,
    timestamp = utils.getTimestamp(options.timestamp)
  }}
  return cloud2murano.trigger(identity, "data_in", payload, options)
end

function cloud2murano.isShadowTopic(topic)
  if topic:sub(1, 11) == "$aws/things" then
    return true
  end
  return false
end
function cloud2murano.useShadow(data, topic)
  local result_message = {}
  -- fill this object to represent a device if it is a get/accepted topic.
  if topic:sub(-12) == "get/accepted" then
    local ready_data = {}
    ready_data.data_in = data.state and data.state.reported
    ready_data.identity = topic:sub(13,-21)
    ready_data.topic = topic
    ready_data.metadata = data.metadata and to_json(data.metadata)
    ready_data.version = data.version
    ready_data.timestamp = data.timestamp
    cloud2murano.validateUplinkDevice(ready_data)
  -- or also if it is update/accepted, an ack resource.
  elseif topic:sub(-15) == "update/accepted" then
    print("Receive part ack: " .. topic .. " " .. to_json(data))
    cloud2murano.validateAckDevice(topic:sub(13,-24), data)
  else
    print("Receive shadow (other): " .. topic .. " " .. to_json(data))
  end
end
function cloud2murano.findIdfromParameters(topic)
  -- call if uplink data does not contains identity, and can detect identity given topic path, as MQtt service client uses wildcard "+" in definition
  -- Can return :
  --  <id> , that is the string matching wildcard "+" if one of the topic defined in Mqtt service client is matching with topic function parameter, and contain just 1 "+"
  --   <full topic> if there is a topic in mqtt service client containing a least 2 wildcard "+"
  --   <full topic> if there is a topic in mqtt without any wildcard "+" but matching with topic function parameter
  for _,topic_sub in pairs(Config.getParameters({service = "mqtt"}).parameters.topics) do
    local index = findplus(topic_sub)
    if index ~= nil then
      local too_much = findplus(topic_sub:sub(1, index-1))
      if too_much ~= nil then
        return topic
      end
      if string.sub(topic_sub, 1, index - 1) == string.sub(topic, 1, index - 1) then
        -- if match with first part ( before "+" wildcard comparison)
        if topic_sub:sub(index+1) == "" or topic_sub:sub(index + 1) == nil then
          -- if no second part
          return topic:sub(index)
        end
        local sec_part = topic_sub:sub(index + 1)
        local long_rel = topic:sub(index):find("/") and topic:sub(index):find("/") - 1
        -- long_rel is the relative length of the identifier in topic
        if topic:sub(-#(sec_part)) == sec_part and (index + long_rel -1 + #sec_part) == #topic then
          return topic:sub(index, index + long_rel - 1)
        end
      end
    else
      if topic == topic_sub then
        return topic
      end
    end
  end
  return nil
end

function cloud2murano.lookUpTopicinTransform(data)
  if data.topic ~= nil then
    -- look if the uplink_topic key from array of channels (uplink_decoding object) ends same as topic
    return findSameTopicInTable(transform.uplink_decoding, data.topic)
  end
  return nil
end

function cloud2murano.lookUpTopicinCache(data)
  -- if there is a match between end of topic and an uplink suffix topic, should return channel
  if data.topic ~= nil then
    return c.getChannelUseCache(data.topic)
  end
  return nil 
end

function cloud2murano.findRegexFromDevicesList(cloud_data_array)
  -- regex to match all name of device from a batch event for ex. useful in vmcache
  local my_dev_id = "^("
  local last_device = ""
  for k, tab_mess in pairs(cloud_data_array) do
    if k == 1 then
      my_dev_id = my_dev_id .. tab_mess.identity
    else
      -- make sure not to add same device in regex
      if last_device ~= tab_mess.identity then
        my_dev_id = my_dev_id .. "|" ..tab_mess.identity
      end
    end
    last_device = tab_mess.identity
  end
  my_dev_id = my_dev_id .. ")$"
  return my_dev_id
end


function cloud2murano.validateAckDevice(identity, uplink_data)
  local ready_data = {ack_meta = {}}
  ready_data.identity = identity
  ready_data.ack_meta.metadata = uplink_data.metadata and to_json(uplink_data.metadata)
  ready_data.ack_meta.version = uplink_data.version
  ready_data.ack_meta.timestamp = uplink_data.timestamp
  ready_data.ack_meta.state = uplink_data.state and to_json(uplink_data.state)
  return cloud2murano.data_in(ready_data.indentity, ready_data, options)
end

function cloud2murano.validateUplinkDevice(uplink_data)
  --generate an additional message if data_in is updated. in all case set uplink_meta state for device.
  print("Receive part : " .. uplink_data.topic .. " " .. to_json(uplink_data))
  local final_state = {}
  final_state.identity = uplink_data.identity
  final_state.data_in = uplink_data.data_in
  final_state.uplink_meta = uplink_data
  final_state.uplink_meta.data_in = nil
  -- Supported types by this example are the above 'provisioned' & 'deleted' functions
  local handler = cloud2murano[final_state.type] or cloud2murano.data_in
  -- Assumes incoming data by default
  return handler(final_state.identity, final_state, options)
end

-- Callback Handler
-- Parse a data from 3rd part cloud into Murano event
-- Support only batch event, see Mqtt batch.message object !
function cloud2murano.callback(cloud_data_array)
  -- Handle batch update
  local result_tot = {}
  -- dedicated for devices in uplink message that need to decode after regenerating their cache
  local device_to_upd = {}
  for k, cloud_data in pairs(cloud_data_array) do
    local data = from_json(cloud_data.payload)
    if cloud2murano.isShadowTopic(cloud_data.topic) then
      data = cloud2murano.useShadow(data, cloud_data.topic)
    else
      data.identity = data.identity or cloud2murano.findIdfromParameters(cloud_data.topic)
      if data.identity ~= nil then
        data.topic = cloud_data.topic
        local topic_from_transform = cloud2murano.lookUpTopicinTransform(data)
        if topic_from_transform ~= nil then
          -- will use transform as it found the matching uplink topic 
          -- set decoded message in data_in resource as json
          data.data_in = transform.data_in and transform.data_in(data, topic_from_transform)
          if data.data_in == nil then
            log.warn('Cannot find transform function data_in, should uncomment')
          end
          result_tot[k] = cloud2murano.validateUplinkDevice(data)
        else
          -- lookup configio, extract a channel if in configio associated uplink topic is matching with topic (same ending)
          local matched_channel_cache = cloud2murano.lookUpTopicinCache(data)
          if matched_channel_cache ~= nil then
            if matched_channel_cache ~= '' then
              for key, value in pairs(data) do
                if key ~= "identity" and key ~= "timestamp" and key ~= "topic" then
                  data.data_in = {}
                  data.data_in[matched_channel_cache] = value
                  data.data_in = to_json(data.data_in)
                  break
                end
              end
              if data.data_in == nil then
                log.warn('Cannot find resource from payload, should verify configIO')
              end
              result_tot[k] = cloud2murano.validateUplinkDevice(data)
            end
          else
            table.insert(device_to_upd, data)
          end
        end
      end
    end
  end
  if #device_to_upd > 0 then
    -- fille a cache independently , reserved for uplink topics. 
    for k, data in pairs(device_to_upd) do
      c.fillCacheOneTopic(data.topic, configIO)
    end
    local regex = cloud2murano.findRegexFromDevicesList(device_to_upd)
    local options = {
      regex = regex,  
    }
    c.cacheFactory(options)
  end
  -- make it again with cache
  for k, data in pairs(device_to_upd) do
    local matched_channel_cache = cloud2murano.lookUpTopicinCache(data)
    if matched_channel_cache ~= nil then
      if matched_channel_cache ~= '' then
        for key, value in pairs(data) do
          if key ~= "identity" and key ~= "timestamp" and key ~= "topic" then
            data.data_in = {}
            data.data_in[matched_channel_cache] = value
            data.data_in = to_json(data.data_in)
            break
          end
        end
        if data.data_in == nil then
          log.warn('Cannot find resource from payload, should verify configIO')
        end
        result_tot[k] = cloud2murano.validateUplinkDevice(data)
      else
        -- should do something for ack, or downlink
        print("Receive part (maybe downlink): " .. data.topic .. " " .. to_json(data))
      end
    end    
  end
  return result_tot
end

return cloud2murano
