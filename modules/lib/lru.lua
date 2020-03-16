-- LRU is a module dedicated for cache, rely on Least Recently used
-- see documentation here : https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_recently_used_(LRU)
-- this is used to limit size of cache, with a limited array of cached key-values. least recently used is removed when try to store a new one after reach size limit.


local lru_cache = {}


function lru_cache.new(size, time_valid_ttl, use_keystore, getter)
  -- info : TTL = time to leave
  -- local table for constructor
  local lru_instance = {}
  if not size or not time_valid_ttl then
    return {error = "no size or TTL configured.."}
  end
  -- size 
  lru_instance.__max_size = size
  -- time cache is valid
  lru_instance.__valid_time = time_valid_ttl
  -- use of keystore
  lru_instance.__use_of_keystore = use_keystore or false
  -- getter  function that can be called during cache operation to get value
  lru_instance.__getter = getter
  -- table with cache : key/value
  lru_instance.__storage = {}
  -- newest
  lru_instance.__newest = 0
  -- oldest
  lru_instance.__oldest = 1
  -- counter for avoid costly call to length of listing_num
  lru_instance.__listing_length = 0
  -- array of keys
  lru_instance.__lru_array = {}


  function lru_instance.setInLuaCache(key, value, optional_TTL)
    if optional_TTL == 0 then
      lru_instance.__storage[key] = {
        ex = 0, -- Expires never 
        value = value,
        newest = lru_instance.__oldest
      }
    else
      lru_instance.__storage[key] = {
        ex = os.time() + (optional_TTL or lru_instance.__valid_time), -- Expires
        value = value,
        newest = lru_instance.__oldest
      }
    end
  end
  function lru_instance.setInLuaCacheEnd(key, value, optional_TTL)
    -- similar to setInLuaCache but update newest attribute, as it is added at newest position
    if optional_TTL == 0 then
      lru_instance.__storage[key] = {
        ex = 0, -- Expires never 
        value = value,
        newest = lru_instance.__newest
      }
    else
      lru_instance.__storage[key] = {
        ex = os.time() + (optional_TTL or lru_instance.__valid_time), -- Expires
        value = value,
        newest = lru_instance.__newest
      }
    end
  end

  function lru_instance.setInKeystore(key, value, optional_TTL)
    --can set a new value, depending some parameters
    if p == 0 then
      Keystore.command(
        {
          key = key,
          command = "set",
          args = {value}
        }
      )
    else
      Keystore.command(
        {
          key = key,
          command = "set",
          args = {value, "EX", (optional_TTL or lru_instance.__valid_time)}
        }
      )
    end
  end
  -- classic calls
  function lru_instance.generateLRU(key, value, optional_TTL)

    if lru_instance.__listing_length == lru_instance.__max_size then
      -- remove oversize cache value, oldest value
      if lru_instance.__storage[lru_instance.__lru_array[lru_instance.__oldest]].newest == lru_instance.__oldest and lru_instance.__lru_array[lru_instance.__oldest] ~= key then
        lru_instance.__storage[lru_instance.__lru_array[lru_instance.__oldest]] = nil
      end
      -- change new value
      lru_instance.__lru_array[lru_instance.__oldest] = key
      lru_instance.setInLuaCache(key, value, optional_TTL)
      if lru_instance.__oldest == lru_instance.__max_size then
        lru_instance.__newest = lru_instance.__newest + 1
        lru_instance.__oldest = 1
      elseif lru_instance.__newest == lru_instance.__max_size then
        lru_instance.__oldest = lru_instance.__oldest + 1
        lru_instance.__newest = 1
      else
        lru_instance.__newest = lru_instance.__oldest
        lru_instance.__oldest = lru_instance.__oldest + 1
      end
    else
      lru_instance.__newest = lru_instance.__newest + 1
      lru_instance.__listing_length = lru_instance.__listing_length + 1
      lru_instance.__lru_array[lru_instance.__newest] = key
      lru_instance.setInLuaCacheEnd(key, value, optional_TTL)
    end
  end

  -- custom logic for lru
  function lru_instance.get(key, optional_TTL)
    -- ttl = time to leave, if value not in lua cache ( or expired) but in other cache (Keystore, getter), will set value with this optional_TTL timeout
    local now = os.time()
    if not lru_instance.__storage[key] or (lru_instance.__storage[key].ex < now and lru_instance.__storage[key].ex > 0) then
      local value = nil
      if lru_instance.__use_of_keystore then
        value = Keystore.get({key = key})
      end
      if value and value.value then
        value = value.value
      else
        -- If not in Keystore, look in a third party cache
        if lru_instance.__getter then
          value = lru_instance.__getter(key, optional_TTL)
        end
      end
      if value == nil or value.error or (type(value) == "table" and #value <= 0) then
        return nil
      end
      if lru_instance.__getter and lru_instance.__use_of_keystore then
        -- means getter got value, so will update keystore
        lru_instance.setInKeystore(key, value, optional_TTL)
      end
      lru_instance.generateLRU(key, value, optional_TTL)
    end
    return lru_instance.__storage[key].value
  end

  function lru_instance.set(key, value, optional_TTL)
    -- ttl = time to leave
    if lru_instance.__storage[key] == value then
      return
    end
    local result = nil
    if lru_instance.__use_of_keystore then
      result = lru_instance.setInKeystore(key, value, optional_TTL)
    end
    if result and result.error then
      return nil
    end
    lru_instance.generateLRU(key, value, optional_TTL)
    return value
  end
  return lru_instance
end

return lru_cache
