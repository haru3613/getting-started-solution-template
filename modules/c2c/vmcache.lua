-- This module cache value to local map
-- To be usefull this function requires the solution to enable the HotVM setting

-- ALL the cache is organized like this : 
--to make sure there is primary keys only
-- Key = <id_device> + "top" = uplink topic from device identity
-- Key = <id_device> + "ch" + <channel>  = port values from device identity depending channel
-- Key = <id_device> + "por" + <port> = channel values from device identity depdending port
-- Key = <id_device> + "ack" = acknowledgment topic from device identity
-- Key = <id_device> + "downlink" + <channel> = mapping downlink topic specific to identity and channel name. 

-- TODO add size limitations/lru/background deletion options
local device2 = murano.services.device2
-- maximum size of LRU cache
local size_lru = 25000
local use_keystore = true
-- time out for cache
local timeout = 30
local lru = require("lib.lru").new(size_lru, timeout, use_keystore)
local cache = {}

function parseKey(json_doc)
  for key,value in pairs(from_json(json_doc)) do
    return key
  end
  return nil
end

function findplus(address)
  return address:match'^.*()+'
end

function populateCacheConfigIO(my_config_io, identity, keyst_cache_timeout)
  -- can be either exosense set config io or basic vendor.configIO file here, in a table format
  for channel, prop in pairs(my_config_io.channels) do
    if prop.protocol_config and prop.protocol_config.app_specific_config then
      local port, downlink_topic, ack_topic = nil,nil,nil
      local activate_control = prop.properties.control or false
      if prop.protocol_config.app_specific_config.port then
        port = tostring(prop.protocol_config.app_specific_config.port)
        if activate_control then
          lru.set(identity .. "ch" .. channel, port, keyst_cache_timeout)
        end
        lru.set(identity .. "por" .. port, channel, keyst_cache_timeout)
      end
      if prop.protocol_config.app_specific_config.downlink_topic and activate_control then
        local raw_downlink = prop.protocol_config.app_specific_config.downlink_topic
        local index_plus = findplus(raw_downlink)
        if index_plus ~= nil then
          -- If there is a plus, change it to replace field with identity, to generate an explicit downlink topic
          downlink_topic = string.sub(raw_downlink, 0, index_plus-1) .. identity .. string.sub(raw_downlink, index_plus+1)
        else
          downlink_topic = raw_downlink
        end
        lru.set(identity .. "downlink" .. channel, downlink_topic, keyst_cache_timeout)
      end
      if prop.protocol_config.app_specific_config.ack_topic and activate_control then
        local raw_ack = prop.protocol_config.app_specific_config.ack_topic
        local index_plus = findplus(raw_ack)
        if index_plus ~= nil then
          -- If there is a plus, change it to replace field with identity, to generate an explicit downlink topic
          ack_topic = string.sub(raw_ack, 0, index_plus-1) .. identity .. string.sub(raw_ack, index_plus+1)
        else
          ack_topic = raw_ack
        end
        lru.set(identity .. "ack", ack_topic, keyst_cache_timeout)
      end
    end
  end
end

function fillCacheOneDevice(identity, uplink_meta, config_io, keyst_cache_timeout)
  if config_io and config_io.reported and config_io.reported:sub(1, 2) ~= "<<" then
    populateCacheConfigIO(from_json(config_io.reported),identity, keyst_cache_timeout)
  else
    -- look for original file in ConfigIO.lua, but value can be wrong
    config_io = require("vendor.configIO")
    if config_io and config_io.config_io and from_json(config_io.config_io) then
      populateCacheConfigIO(from_json(config_io.config_io),identity, keyst_cache_timeout)
    end
  end
  if uplink_meta and uplink_meta.reported and from_json(uplink_meta.reported) and from_json(uplink_meta.reported).topic then
    lru.set(identity .. "top", from_json(uplink_meta.reported).topic, keyst_cache_timeout)
  end
end

function cache.cacheFactory(options)
  -- will call for all device: fillCacheOneDevice
  print("Regenerating cache ...")
  local keyst_cache_timeout = os.getenv("KEYSTORE_TIMEOUT") or 300
  if options == nil then
    options = {}
  end
  if options.singledevice and options.identity then
    -- faster as just one device to update
    local reported = device2.getIdentityState({identity = options.identity})
    local uplink_meta = reported.uplink_meta
    local config_io = reported.config_io
    fillCacheOneDevice(options.identity, uplink_meta, config_io, keyst_cache_timeout)
  else
    local query = {
      -- if regex nil value, no filter ! 
      -- filter can be a regex to match all name of device from a batch event for ex.
      identity = options.regex
    }
    local reported = device2.listIdentities(query)
    if reported.devices then
      for k,v in pairs(reported.devices) do
        local identity = v.identity
        -- should be defined after set in exosense
        local config_io = v.state.config_io
        -- should be defined after first uplink
        local uplink_meta = v.state.uplink_meta
        fillCacheOneDevice(identity, uplink_meta, config_io, keyst_cache_timeout)
      end
    end
  end
end

function cache.getTopicUseCache(identity)
  local topic = lru.get(identity .. "top")
  return topic
end

function cache.getDownlinkUseCache(identity, channel_data_out)
  --return : downlink topic. They are taken from cache, or config_io module.
  -- as config_io contains general data model definition , need to create corresponding device topic with it and with device id.
  local downlink_topic = lru.get(identity .. "downlink" .. channel_data_out)
  if downlink_topic == nil then
    local options = {
      -- Specify not to update all devices, so call for device2 will be different (just GetIdentityState)
      singledevice = true,
      identity = identity
    }
    cache.cacheFactory(options)
    downlink_topic = lru.get(identity .. "downlink" .. channel_data_out)
  end
  return downlink_topic
end

function cache.getAckTopicUseCache(identity)
  -- return acknowledgment topic for device id
  local ack_topic = lru.get(identity .. "ack")
  return ack_topic
end

-- overload vmcache for MQTT senseway
function cache.getChannelUseCache(data_device_type_uplink)
  -- return : channel taken from cache, or nil 
  -- depends port value. Please, remember: port can be set to 1 if no field in uplink
  local channel = lru.get(data_device_type_uplink.identity .. "por" .. tostring(data_device_type_uplink.port))
  return channel
end

function cache.getPortUseCache(data_device_downlink)
  --return :  port. They are taken from cache
  local channel = parseKey(data_device_downlink.data_out)
  local port = nil
  if channel ~= nil then
    port = lru.get(data_device_downlink.identity .. "ch" .. channel)
  end
  return port
end


return cache
