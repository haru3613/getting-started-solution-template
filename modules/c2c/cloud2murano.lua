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
  return false
end

function cloud2murano.getIdentityTopic(string_topic)
  local temp = string.sub(string_topic,0,string_topic:match'^.*()/'-1)
  return string.sub(temp, temp:match'^.*()/'+1)
end

function cloud2murano.detectUplink(string_topic)
  local last_part = string.sub(string_topic, string_topic:match'^.*()/')
  local service_mqtt = Config.getParameters({service = "mqtt"}).parameters
  local topic_suffix = '/uplink'
  if service_mqtt.topics and #(service_mqtt.topics)>0 then
    -- can be stored as topic parameters from mqtt service, in first element.
    topic_suffix = string.sub(service_mqtt.topics[1], service_mqtt.topics[1]:match'^.*()/')
  end
  if last_part == topic_suffix then
    return true
  end
  return false
end
function cloud2murano.detectAck(string_topic)
  local last_part = string.sub(string_topic, string_topic:match'^.*()/')
  local service_mqtt = Config.getParameters({service = "mqtt"}).parameters
  local topic_suffix = '/ack'
  if service_mqtt.topics and #(service_mqtt.topics)>2 then
    -- can be stored as topic parameters from mqtt service, in first element.
    topic_suffix = string.sub(service_mqtt.topics[3], service_mqtt.topics[3]:match'^.*()/')
  end
  if last_part == topic_suffix then
    return true
  end
  return false
end

function cloud2murano.printUplink(elem)
  print(elem .. " : data_in updated.")
end

function cloud2murano.validateUplinkDevice(uplink_data)
  local final_state = {}
  final_state.identity = uplink_data.identity
  final_state.data_in = transform.data_in and transform.data_in(uplink_data)
  if final_state.data_in == nil then
    log.warn('Cannot find transform module, should uncomment module')
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
function cloud2murano.callback(cloud_data_array, options)
  -- Handle batch update
  local result_tot = {}
  -- dedicated for devices in uplink message that need to decode after regenerating their cache
  local device_to_upd = {}
  for k, cloud_data in pairs(cloud_data_array) do
    local data = from_json(cloud_data.payload)
    local final_state = {}
    if cloud2murano.detectUplink(cloud_data.topic) then
      data.identity = data.identity or cloud2murano.getIdentityTopic(cloud_data.topic)
      data.topic = cloud_data.topic
      --if you don't specify port, use operation mapping port 1.
      local port = data.port or 1
      data.port = port
      data.channel= c.getChannelUseCache(data)
      if data.channel ~= nil then
        print("Receive part: " .. cloud_data.topic .. " " .. cloud_data.payload)
        result_tot[k] = cloud2murano.validateUplinkDevice(data)
      else
        table.insert(device_to_upd, data)
      end
    else
      print("Receive part (downlink): " .. cloud_data.topic .. " " .. cloud_data.payload)
      -- should implement a way to detect ack messages, topic would map a third argument in topics from mqtt service parameters
      if cloud2murano.detectAck(cloud_data.topic) then
        -- because a acknowledgment is caught, store it in ack_meta resource
        cloud2murano.setAckResource(data)
      end
      result_tot[k] = {message = "Is a Downlink"}
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
    data.channel = c.getChannelUseCache(data)
    if data.channel == nil then
      log.warn("Cannot find channels configured for this port: " .. tostring(data.port))
    end
    print("Receive after re-generate cache from device: " ..  data.identity)
    cloud2murano.validateUplinkDevice(data)
  end
  return result_tot
end

return cloud2murano
