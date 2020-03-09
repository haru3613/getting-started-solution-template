local murano2cloud = {}
-- This module maps local changes and propagate them to the 3rd party cloud

local device2  = murano.services.device2
local transform = require("vendor.c2c.transform")
local cloud2murano = require("c2c.cloud2murano")
local c = require("c2c.vmcache")

-- create a random ref for downlink
function rand_bytes(length)
  local res = ""
  for i = 1, length do
      res = res .. string.char(math.random(97, 122))
  end
  return res
end

function dummyEventAcknowledgement(operation) 
  --  Need to confirm to exosense like device got Mqtt downlink, by creating this fake event
  local payload = {{
    -- it relies on data_out
    values = {data_out = operation.data_out},
    timestamp = os.time()*1000000
  }}
  local event = {
    type = "data_in",
    identity = operation.identity,
    timestamp = os.time()*1000000,
    payload = payload
  }
  if handle_device2_event then
    return handle_device2_event(event)
  end
end

-- function which is the real setIdentityState, dedicated for data_out 
function murano2cloud.updateWithMqtt(data, topic, port)
  local table_result = transform.data_out and transform.data_out(from_json(data.data_out)) -- template user customized data transforms
  if table_result ~= nil then
    local data_downlink = {}
    local message, error = device2.setIdentityState(data)
    if error then
      log.error(error)
      return false
    end
     -- As data is just the small message to send, need to get some meta data to publish to tx
    data_downlink = {
      ["cnf"] = table_result.cnf,
      -- Auto-generated
      ["ref"] = rand_bytes(12),
      ["port"] = port,
      ["data"] = table_result.data
    }
    Mqtt.publish({messages={{topic = topic, message = data_downlink}}})
    -- create fake event to simulate acknowledgment, fast and blindness logic.
    -- otherwise would wait for acknowledgment on /ack topic, and inside fields should match with ref field of downlink and identity of device
    return dummyEventAcknowledgement(data)
  else
    log.error("Didn't send any Downlink: no Transform configured.")
    return false
  end
end

-- Below function uses the operations of device2, overload it.
-- See all operations available in http://docs.exosite.com/reference/services/device2,
-- but will need to associate a custom interface ( see services/interface/configure_operations)
-- data should be incoming data from Exosense
function murano2cloud.setIdentityState(data)
  if data.identity ~= nil then
    if data.config_io ~= nil then
      --just update config_io here
      return device2.setIdentityState(data)
    end
    if data.data_out ~= nil then
      --specific to value in data_out, a port is associated, details in config_io channels, on exosense
      local old_topic,port_config_io = c.getTopicPortUseCache(data)
      if old_topic == nil then
        log.error("Didn't send any Downlink: no Uplink got initially")
      elseif port_config_io == nil then
        log.error("Didn't send any Downlink: no matching values from channels in configIO and data_out, or no port added")
      else
        local downlink_topic = string.sub(old_topic, 0, old_topic:match'^.*()/').."tx"
        return murano2cloud.updateWithMqtt(data, downlink_topic, port_config_io)
      end
    end
  end
end

-- Function for recurrent pool action
function murano2cloud.syncAll(data)
  if data ~= nil then
    return Device2.getIdentityState(data)
  end
  return nil
end

return murano2cloud
