-- This module cache value to local map
-- To be usefull this function requires the solution to enable the HotVM setting

-- ALL the cache is organized like this : 
--to make sure there is primary keys only
-- Key = "top" + <topic>  = channel value matching with topic entry
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

function findplus(address)
  return address:match'^.*()+'
end

function cache.AddressMatchWith(str, generic_path)
  -- function to detect if address candidate is same as address possessing a wildcard.
  -- wildcard is remplaced with identity.
  if generic_path ~= nil then
    local index = findplus(generic_path)
    if index == nil then
      return str == generic_path
    end
    return str:sub(1, #(generic_path:sub( 1, index-1))) == generic_path:sub( 1, index-1) and (generic_path:sub(index+1) == "" or str:sub(-#(generic_path:sub(index+1))) == generic_path:sub(index+1))
  end
  return false
end


function populateCacheConfigIO(my_config_io, identity, actual_uplink, keyst_cache_timeout)
  -- can be either exosense set config io or basic vendor.configIO file here, in a table format
  for channel, prop in pairs(my_config_io.channels) do
    if prop.protocol_config and prop.protocol_config.app_specific_config then
      local downlink_topic = nil
      local activate_control = prop.properties.control or false
      if prop.protocol_config.app_specific_config.uplink_topic and cache.AddressMatchWith(actual_uplink, prop.protocol_config.app_specific_config.uplink_topic) then
        -- a uplink topic is matched with a channel, if field uplink_topic corresponds to actual_uplink (candidate)
        lru.set("top" .. actual_uplink, channel, keyst_cache_timeout)
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
    end
  end
end

function fillCacheOneDevice(identity, uplink_meta, config_io, keyst_cache_timeout)
  if uplink_meta and uplink_meta.reported and from_json(uplink_meta.reported) ~= nil and from_json(uplink_meta.reported).topic then
    local actual_uplink = from_json(uplink_meta.reported).topic
    if config_io and config_io.reported and config_io.reported:sub(1, 2) ~= "<<" then
      populateCacheConfigIO(from_json(config_io.reported),identity, actual_uplink, keyst_cache_timeout)
    else
      -- look for original file in ConfigIO.lua, but value can be wrong
      config_io = require("vendor.configIO")
      if config_io and config_io.config_io and from_json(config_io.config_io) ~= nil then
        populateCacheConfigIO(from_json(config_io.config_io),identity, actual_uplink, keyst_cache_timeout)
      else
        log.warn('Bad ConfigIO file')
      end
    end
  end
end

function cache.fillCacheOneTopic(topic, configIO)
  local keyst_cache_timeout = os.getenv("KEYSTORE_TIMEOUT") or 300
  if configIO and configIO.config_io and from_json(configIO.config_io) ~= nil then
    local channels = from_json(configIO.config_io)
    for channel, prop in pairs(channels.channels) do
      if prop.protocol_config and prop.protocol_config.app_specific_config and prop.protocol_config.app_specific_config.uplink_topic and cache.AddressMatchWith(topic, prop.protocol_config.app_specific_config.uplink_topic) then
        -- found a channel configured for this uplink topic. It shouldn't have other channel, thats why directly returning
        lru.set("top" .. topic, channel, keyst_cache_timeout)
        return
      end
    end
    -- no channel matching with topic, so set it to ''
    lru.set("top" .. topic, '', keyst_cache_timeout)
  else
    log.warn('Bad ConfigIO file')
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

-- overload vmcache for MQTT 
function cache.getChannelUseCache(topic)
  -- return : channel taken from cache, or nil 
  -- depends topic value.
  local channel = lru.get("top" .. topic )
  return channel
end


return cache
