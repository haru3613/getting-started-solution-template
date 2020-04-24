local murano2cloud = {}
-- This module maps local changes and propagate them to the 3rd party cloud

local device2  = murano.services.device2
local transform = require("vendor.c2c.transform")
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
function murano2cloud.isShadowTopic(topic)
  if topic:sub(1, 4) == "$aws" and (topic:sub(-6) == "update" or topic:sub(-7) == "update/") then
    return true
  else
    return false
  end
end
function murano2cloud.prepareShadowUpdate(data_out)
  local table_shadow_update = {state= {desired = {}}}
  for channel, v in pairs(data_out) do
    table_shadow_update.state.desired.channel = v
  end
  return table_shadow_update
end

-- function which is the real setIdentityState, dedicated for data_out and sends mqtt message then, eventually after encode value
function murano2cloud.updateWithMqtt(data, topic)
  local message, error = device2.setIdentityState(data)
  if error then
    log.error(error)
    return false
  end
  data_out = from_json(data.data_out)
  -- this will be send to device through downlink topic
  local data_downlink = {}
  data_downlink.identity = data.identity
  local channel = next(data_out)
  if channel ~= nil then
    if murano2cloud.isShadowTopic(topic) then
      -- case with shadow, so that a specific structure is set
      data_downlink = murano2cloud.prepareShadowUpdate(data_out)
    else
      -- add some custom part to downlink message, given transform module
      local table_encoded = transform.data_out and transform.data_out(channel, data_out[channel]) -- template user customized data transforms
      if table_encoded == nil then
        table_encoded = {}
        table_encoded[channel] = data_out[channel]
        log.warn("no Transform configured, didn't encode values.")
      end
      for k, v in pairs(table_encoded) do
        data_downlink[k] = v
      end
    end
    local published = Mqtt.publish({messages={{topic = topic, message = data_downlink}}})
    if published.error then
      return false
    end
    -- create fake event to simulate acknowledgment, fast and blindness logic.
    -- otherwise would wait for acknowledgment on /ack topic, and inside fields should match with ref field of downlink and identity of device
    return dummyEventAcknowledgement(data)
  end
  return false
end

-- Below function uses the operations of device2, overload it.
-- See all operations available in http://docs.exosite.com/reference/services/device2,
-- but will need to associate a custom interface (see services/interface/configure_operations)
-- data should be incoming data from Exosense
function murano2cloud.setIdentityState(data)
  if data.identity ~= nil then
    if data.config_io ~= nil then
      --just update config_io here
      return device2.setIdentityState(data)
    end
    if data.data_out ~= nil then
      local converted_values = from_json(data.data_out)
      if converted_values then
        local channel = next(converted_values)
        local topic_downlink = c.getDownlinkUseCache(data.identity, channel)
        -- a downlink topic must be described in configIO report to README
        -- by the way, cache has been eventually refreshed during previous command
        if topic_downlink ~= nil then
          -- updateWithMqtt : will eventually encode value before publish
          return murano2cloud.updateWithMqtt(data, topic_downlink)
        else
          --no topic, means nothing to do 
          log.error("No Downlink, no downlink topic found in Config IO for this channel." .. channel)
        end
      else
        log.error("Data_out values are not in JSON format")
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
