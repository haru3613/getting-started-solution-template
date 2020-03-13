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
      my_dev_id = my_dev_id .. tab_mess.mod.devEUI
    else
      -- make sure not to add same device in regex
      if last_device ~= tab_mess.mod.devEUI then
        my_dev_id = my_dev_id .. "|" ..tab_mess.mod.devEUI
      end
    end
    last_device = tab_mess.mod.devEUI
  end
  my_dev_id = my_dev_id .. ")$"
  return my_dev_id
end

function cloud2murano.detect_uplink(string_topic)
  local topic = string.sub(string_topic, string_topic:match'^.*()/')
  if topic == '/rx' then
    return true
  end
  return false
end
function cloud2murano.print_downlink(elem)
  if elem ~= nil then
    print("data_in not updated from: " .. elem .. ". Not an uplink")
  else
    print("data_in not updated. Not an uplink")
  end
end
function cloud2murano.print_uplink(elem)
  print(elem .. " : data updated.")
end

function cloud2murano.validateUplinkDevice(uplink_data)
  local final_state = {}
  final_state.identity = uplink_data.mod.devEUI
  final_state.data_in = transform.data_in and transform.data_in(uplink_data)
  if final_state.data_in == nil then
    log.warn('Cannot find transform module, should uncomment module')
  end
  -- Need to save some metadata
  final_state.lorawan_meta = uplink_data
  -- remove part channel here, no needed anymore
  final_state.lorawan_meta.channel = nil
  cloud2murano.print_uplink(final_state.identity)

  -- Supported types by this example are the above 'provisioned' & 'deleted' functions
  local handler = cloud2murano[final_state.type] or cloud2murano.data_in
  -- Assumes incoming data by default
  return handler(final_state.identity, final_state, options)
end


-- Callback Handler
-- Parse a data from 3rd part cloud into Murano event
-- Support only batch event, see Mqtt batch.message object
function cloud2murano.callback(cloud_data_array,options)
  -- Handle batch update
  local result_tot = {}
  -- dedicated for devices in uplink message that need to decode after regenerating their cache
  local device_to_upd = {}

  -- First loop : first time for all type of data. ... For those uplink and during decoding part, if no channel linked with port, report these devices in a third time job for decoding, just after update cache.
  for k, cloud_data in pairs(cloud_data_array) do
    local data = from_json(cloud_data.payload)
    if cloud2murano.detect_uplink(cloud_data.topic) then
      if data.mod.devEUI then
        -- Transform will parse data, depending channel value -got from port-
        -- Decoding logic can handle several channel linked with same port, just configure it in transform.uplink_decoding 
        data.topic = cloud_data.topic
        data.channel = c.getChannelUseCache(data)
        if data.channel ~= nil then
          print("receive part: " .. cloud_data.topic .. " " .. cloud_data.payload)
          result_tot[k] = cloud2murano.validateUplinkDevice(data)
        else
          -- keep all data, and it is already a lua table
          table.insert(device_to_upd, data)
          result_tot[k] = {message = "For this uplink : Not found matching channel with topic or get from cache"}
        end
      else
        log.warn("Cannot find identity in uplink payload..", to_json(data))
        result_tot[k] = {error = "Cannot find identity in uplink payload.."}
      end
    else
      print("receive part: " .. cloud_data.topic .. " " .. cloud_data.payload)
      cloud2murano.print_downlink(data.mod.devEUI)
      result_tot[k] = {message = "Is a Downlink"}
    end
  end
  -- Condition to update cache on uplink devices, it will be just one time
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
      log.warn("Cannot find channels configured for this port of this device in configIO")
    end
    print("receive after re-generate cache : " .. data.topic .. " " .. to_json(data))
    cloud2murano.validateUplinkDevice(data)
  end
  return result_tot
end

return cloud2murano
