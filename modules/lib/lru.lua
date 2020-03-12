-- LRU is a module dedicated for cache, rely on Least Recently used
-- see documentation here : https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_recently_used_(LRU)
-- this is used to limit size of cache, with a limited array of cached key-values. least recently used is removed when try to store a new one after reach size limit.


local lru_cache = {}


function lru_cache.new(size)
  --local table for constructor
  local lru_instance = {}
  -- size 
  lru_instance.__max_size = size
  -- newest 
  lru_instance.__newest = 0
  -- oldest
  lru_instance.__oldest = 1
  -- array of keys
  lru_instance.__listing_num = {}

  -- classic calls
  function lru_instance.generateLRU(key, value)
    local now = os.time()
    -- maybe copy #lru_instance.__listing_num in a separate var for performance
    if #lru_instance.__listing_num == lru_instance.__max_size then
      -- remove oversize cache value, oldest value
      lru_instance[lru_instance.__listing_num[lru_instance.__oldest]] = nil
      -- change new value
      lru_instance.__listing_num[lru_instance.__oldest] = key
      lru_instance[key] = {
        ex = now + 30, -- Expires
        value = value
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
      lru_instance.__listing_num[lru_instance.__newest] = key
      lru_instance[key] = {
        ex = now + 30, -- Expires
        value = value
      }
    end
  end
  -- custom logic for lru
  function lru_instance.get(key, getter, timeout)
    local now = os.time()
    if not lru_instance[key] or lru_instance[key].ex < now then
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
        lru_instance[key] = nil
        return nil
      end
      lru_instance.generateLRU(key, value)
    end
    return lru_instance[key].value
  end

  function lru_instance.set(key, value, timeout, setter)
    if lru_instance[key] == value then
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
      lru_instance.generateLRU(key, value)
    return value
  end
  return lru_instance
end

return lru_cache
