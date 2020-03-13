-- LRU is a module dedicated for cache, rely on Least Recently used
-- see documentation here : https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_recently_used_(LRU)
-- this is used to limit size of cache, with a limited array of cached key-values. least recently used is removed when try to store a new one after reach size limit.


local lru_cache = {}


function lru_cache.new(size, getter, setter, time_valid)
  -- local table for constructor
  local lru_instance = {}
  -- getter and setter are two functions that can be called during cache operations
  lru_instance.__getter = getter
  lru_instance.__setter = setter
  -- table with cache : key/value
  lru_instance.__storage = {}
  -- size 
  lru_instance.__max_size = size or 25000
  -- newest 
  lru_instance.__newest = 0
  -- oldest
  lru_instance.__oldest = 1
  -- time cache is valid
  lru_instance.__valid_time = time_valid or 30
  -- counter for avoid costly call to length of listing_num
  lru_instance.__listing_length = 0
  -- array of keys
  lru_instance.__lru_array = {}


  -- classic calls
  function lru_instance.generateLRU(key, value)
    local now = os.time()
    if lru_instance.__listing_length == lru_instance.__max_size then
      -- remove oversize cache value, oldest value
      if lru_instance.__storage[lru_instance.__lru_array[lru_instance.__oldest]].newest == lru_instance.__oldest and lru_instance.__lru_array[lru_instance.__oldest] ~= key then
        lru_instance.__storage[lru_instance.__lru_array[lru_instance.__oldest]] = nil
      end
      -- change new value
      lru_instance.__lru_array[lru_instance.__oldest] = key
      lru_instance.__storage[key] = {
        ex = now + lru_instance.__valid_time, -- Expires
        value = value,
        newest = lru_instance.__oldest
      }
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
      lru_instance.__storage[key] = {
        ex = now + lru_instance.__valid_time, -- Expires
        value = value,
        newest = lru_instance.__newest
      }
    end
  end
  -- custom logic for lru
  function lru_instance.get(key, timeout)
    local now = os.time()
    if not lru_instance.__storage[key] or lru_instance.__storage[key].ex < now then
      local value = Keystore.get({key = key})
      if value and value.value then
        value = value.value
      else
        -- If not in Keystore, look in a third party cache
        if lru_instance.__getter then
          value = lru_instance.__getter(key,timeout)
        end
      end
      if value == nil or value.error or (type(value) == "table" and #value <= 0) then
        return nil
      end
      lru_instance.generateLRU(key, value)
    end
    return lru_instance.__storage[key].value
  end

  function lru_instance.set(key, value, timeout)
    if lru_instance.__storage[key] == value then
      return
    end
    local result =
      Keystore.command(
      {
        key = key,
        command = "set",
        args = {value, "EX", (timeout or lru_instance.__valid_time)}
      }
    )
    if result and result.error then
      return nil
    end
    if lru_instance.__setter then
      -- set also in a third party cache
      lru_instance.__setter(key, value, timeout or lru_instance.__valid_time)
    end
    lru_instance.generateLRU(key, value)
    return value
  end

  return lru_instance
end

return lru_cache
