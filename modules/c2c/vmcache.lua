-- This module cache value to local map
-- To be usefull this function requires the solution to enable the HotVM setting

-- ALL the cache is organized like this : 
--to make sure there is primary keys only
-- Key = <id_device> + "0" = topic values from device identity
-- Key = <id_device> + <channel> + "1" = port values from device identity depending channel
-- Key = <id_device> + <port> + "2" = channel values from device identity depdending port

-- TODO add size limitations/lru/background deletion options
local device2 = murano.services.device2
local cache = {}

function cache.get(key, getter, timeout)
  local now = os.time()
  if not cache[key] or cache[key].ex < now then
    local value
    if getter then
      value = getter(key)
    else
      value = Keystore.get({key = key})
      if value and value.value then
        value = value.value
      end
    end
    if value == nil or value.error or (type(value) == "table" and #value <= 0) then
      cache[key] = nil
      return nil
    end
    cache[key] = {
      ex = now + (timeout or 30), -- Expires
      value = value
    }
  end
  return cache[key].value
end

function cache.set(key, value, timeout, setter)
  if cache[key] == value then
    return
  end

  local result
  if setter then
    result = setter(key, value)
  else
    result =
      Keystore.command(
      {
        key = key,
        command = "set",
        args = {value, "EX", (timeout or 30)}
      }
    )
  end
  if result and result.error then
    return nil
  end
  cache[key] = {
    ex = os.time() + 30, -- Expires
    value = value
  }
  return value
end

function parseKey(json_doc)
  for key,value in pairs(from_json(json_doc)) do
    return key
  end
  return nil
end

function populateCacheChannelAndPort(my_config_io, identity, vm_cache_timeout)
  -- can be either exosense set config io or basic vendor.configIO file here, in a table format
  for channel, prop in pairs(my_config_io.channels) do
    if prop.protocol_config and prop.protocol_config.app_specific_config and prop.protocol_config.app_specific_config.port then
      local port = tostring(prop.protocol_config.app_specific_config.port)
      if prop.properties.control then
        cache.set(identity .. channel .. "1", port, vm_cache_timeout)
      end
      cache.set(identity .. port .. "2", channel, vm_cache_timeout)
    end
  end
end

function fillCacheOneDevice(identity,lorawan_meta,config_io,vm_cache_timeout)
  if config_io and config_io.reported and config_io.reported:sub(1, 2) ~= "<<" then
    populateCacheChannelAndPort(from_json(config_io.reported),identity, vm_cache_timeout)
  else
    -- look for original file in ConfigIO.lua, but value can be wrong
    config_io = require("vendor.configIO")
    if config_io and config_io.config_io and from_json(config_io.config_io) then
      populateCacheChannelAndPort(from_json(config_io.config_io),identity, vm_cache_timeout)
    end
  end
  if lorawan_meta and lorawan_meta.reported and from_json(lorawan_meta.reported) and from_json(lorawan_meta.reported).topic then
    cache.set(identity .. "0", from_json(lorawan_meta.reported).topic, vm_cache_timeout)
  end
end

function cache.cacheFactory(options)
  -- will call for all device: fillCacheOneDevice
  print("Regenerating cache ...")
  vm_cache_timeout = os.getenv("VMCACHE_TIMEOUT") or 300
  if options == nil then
    options = {}
  end
  if options.singledevice and options.identity then
    -- faster as just one device to update
    local reported = device2.getIdentityState({identity = options.identity})
    local lorawan_meta = reported.lorawan_meta
    local config_io = reported.config_io
    fillCacheOneDevice(options.identity,lorawan_meta,config_io,vm_cache_timeout)
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
        local lorawan_meta = v.state.lorawan_meta
        fillCacheOneDevice(identity,lorawan_meta,config_io,vm_cache_timeout)
      end
    end
  end
end

-- overload vmcache for MQTT senseway
function cache.getChannelUseCache(data_device_type_uplink)
  -- return : channel taken from cache
  -- depends port value
  -- give an option if you want to update cache
  local channel
  if data_device_type_uplink.mod and data_device_type_uplink.mod.port then
    local identity = data_device_type_uplink.mod.devEUI
    channel = cache.get(identity .. tostring(data_device_type_uplink.mod.port) .. "2")
  end
  return channel
end

function cache.getTopicPortUseCache(data_device_downlink)
  --return : topic, port. They are taken from cache
  --if any in cache will call cacheFactory and generate it.
  local options = {
    -- Specify not to update all devices, so call for device2 will be different (just GetIdentityState)
    singledevice = true,
    identity = data_device_downlink.identity
  }
  local topic = cache.get(data_device_downlink.identity .. "0")
  local channel = parseKey(data_device_downlink.data_out)
  local port = cache.get(data_device_downlink.identity .. channel .. "1")
  if channel ~= nil then
    if topic == nil or port == nil then
      --regenerate cache, not call each time
      cache.cacheFactory(options)
      topic = cache.get(data_device_downlink.identity .. "0")
      port = cache.get(data_device_downlink.identity .. channel .. "1")
    end
  end
  return topic, port
end


return cache
