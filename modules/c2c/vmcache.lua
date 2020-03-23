-- This module cache value to local map
-- To be usefull this function requires the solution to enable the HotVM setting

-- ALL the cache is organized like this : 
--to make sure there is primary keys only
-- Key = <id_device> + "top" = topic values from device identity
-- Key = <id_device> + "ch" + <channel>  = port values from device identity depending channel
-- Key = <id_device> + "por" + <port> = channel values from device identity depdending port

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

function populateCacheChannelAndPort(my_config_io, identity, keyst_cache_timeout)
  -- can be either exosense set config io or basic vendor.configIO file here, in a table format
  for channel, prop in pairs(my_config_io.channels) do
    if prop.protocol_config and prop.protocol_config.app_specific_config and prop.protocol_config.app_specific_config.port then
      local port = tostring(prop.protocol_config.app_specific_config.port)
      if prop.properties.control then
        lru.set(identity .. "ch" .. channel, port, keyst_cache_timeout)
      end
      lru.set(identity .. "por" .. port, channel, keyst_cache_timeout)
    end
  end
end

function fillCacheOneDevice(identity, uplink_meta, config_io, keyst_cache_timeout)
  if config_io and config_io.reported and config_io.reported:sub(1, 2) ~= "<<" then
    populateCacheChannelAndPort(from_json(config_io.reported),identity, keyst_cache_timeout)
  else
    -- look for original file in ConfigIO.lua, but value can be wrong
    config_io = require("vendor.configIO")
    if config_io and config_io.config_io and from_json(config_io.config_io) then
      populateCacheChannelAndPort(from_json(config_io.config_io),identity, keyst_cache_timeout)
    end
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

-- overload vmcache for MQTT senseway
function cache.getChannelUseCache(data_device_type_uplink)
  -- return : channel taken from cache, or nil 
  -- depends port value. Please, remember: port can be set to 1 if no field in uplink
  local channel = lru.get(data_device_type_uplink.identity .. "por" .. tostring(data_device_type_uplink.port))
  return channel
end

function cache.getPortUseCache(data_device_downlink)
  --return : topic, port. They are taken from cache
  --if any in cache will call cacheFactory and generate it.
  local options = {
    -- Specify not to update all devices, so call for device2 will be different (just GetIdentityState)
    singledevice = true,
    identity = data_device_downlink.identity
  }
  local channel = parseKey(data_device_downlink.data_out)
  if channel ~= nil then
    local port = lru.get(data_device_downlink.identity .. "ch" .. channel)
    if port == nil then
      --regenerate cache, not call each time
      cache.cacheFactory(options)
      port = lru.get(data_device_downlink.identity .. "ch" .. channel)
    end
  end
  return port
end


return cache
