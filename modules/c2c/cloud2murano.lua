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

function cloud2murano.setAckResource(data)
  -- because of acknowledgment, need to store in ack_meta resource from device.
  local final_state = {}
  if data.identity then
    final_state.identity = data.identity
    final_state.ack_meta = to_json(data)
    -- Assumes incoming data by default
    return cloud2murano.data_in(final_state.identity, final_state, options)
  end
end

function cloud2murano.detectAck(string_topic, identity)
  local last_part = string.sub(string_topic, string_topic:match'^.*()/')
  if last_part ~= identity and last_part == '/ack' then
    return true
  end
  return false
end

function cloud2murano.printUplink(elem)
  print(elem .. " : data_in updated.")
end

function cloud2murano.IsAckTopic(topic, identity)
  local ack_top = c.getAckTopicUseCache(identity)
  if ack_top ~= nil and topic == ack_top then
    return true
  end
  return false
end

function cloud2murano.HasDeviceTopicCache(identity)
  local topic = c.getTopicUseCache(identity)
  return cache_extract
end

function cloud2murano.validateUplinkDevice(uplink_data, use_decode)
  --flag use_decode to know if needed to decode value, otherwise set available message in data_in resource as json
  local final_state = {}
  final_state.identity = uplink_data.identity
  if use_decode then
    final_state.data_in = transform.data_in and transform.data_in(uplink_data)
    if final_state.data_in == nil then
      log.warn('Cannot find transform module, should uncomment module')
    end
  else
    local data_in = {}
    for k,v in uplink_data do
      if k ~= "identity" and k ~= "timestamp" and k ~= "topic" then
        data_in[k] = v
      end
    end
    final_state.data_in = to_json(data_in)
  end
  -- Need to save some metadata
  final_state.uplink_meta = uplink_data
  -- remove part channel here, no needed anymore
  final_state.uplink_meta.channel = nil
  cloud2murano.printUplink(final_state.identity)
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
    local final_state = {}
    if data.identity ~= nil then
      data.topic = cloud_data.topic
      local topic_from_cache = cloud2murano.HasDeviceTopicCache(data.identity)
      if topic_from_cache ~= nil then
        if cloud_data.topic == topic_from_cache then
          -- it is an uplink message
          if data.port ~= nil then
            data.channel = c.getChannelUseCache(data)
            if data.channel == nil then
              log.warn("Cannot find channels configured for this port: " .. tostring(data.port))
            end
            print("Receive part: " .. cloud_data.topic .. " " .. cloud_data.payload)
            -- need decode so set to true
            local need_decode = true
            result_tot[k] = cloud2murano.validateUplinkDevice(data, need_decode)
          else
            -- channel name and value are ready to use, no decode !
            cloud2murano.validateUplinkDevice(data, false)
          end
        else
          -- assuming it is a downlink, or even ack message
          local is_ack = cloud2murano.IsAckTopic(cloud_data.topic, data.identity)
          if is_ack then
            print("Receive part (acknowledgement): " .. cloud_data.topic .. " " .. cloud_data.payload)
            cloud2murano.setAckResource(data)
            result_tot[k] = {message = "Is an Acknowledgment"}
          else
            print("Receive part (downlink): " .. cloud_data.topic .. " " .. cloud_data.payload)
            result_tot[k] = {message = "Is a Downlink"}
          end
        end
      else
        -- no cache, should regenerate it. Add to array : device to upd their cache.
        table.insert(device_to_upd, data)
      end
    end
  end
  if #device_to_upd > 0 then
    local regex = cloud2murano.findRegexFromDevicesList(device_to_upd)
    local options = {
      regex = regex
    }
    c.cacheFactory(options)
  end
  -- Third time can decode uplink after updating the cache
  for k, data in pairs(device_to_upd) do
    local topic_from_cache = cloud2murano.HasDeviceTopicCache(data.identity)
    if topic_from_cache ~= nil and topic_from_cache ~= data.topic then
      -- downlink 
      local is_ack = cloud2murano.IsAckTopic(data.topic, data.identity)
      if is_ack then
        print("Receive part (acknowledgement): " .. data.topic .. " " .. to_json(data))
        cloud2murano.setAckResource(data)
        result_tot[k] = {message = "Is an Acknowledgment"}
      else
        print("Receive part (downlink): " .. data.topic .. " " .. to_json(data))
        result_tot[k] = {message = "Is a Downlink"}
      end
    else
      --uplink
      if data.port ~= nil then
        data.channel = c.getChannelUseCache(data)
        if data.channel == nil then
          log.warn("Cannot find channels configured for this port: " .. tostring(data.port))
        end
        print("Receive part: " .. data.topic .. " " .. to_json(data))
        -- need decode so set to true
        local need_decode = true
        result_tot[k] = cloud2murano.validateUplinkDevice(data, need_decode)
      else
        -- channel name and value are ready to use, no decode !
        cloud2murano.validateUplinkDevice(data, false)
      end
    end
  end
  return result_tot
end

return cloud2murano
