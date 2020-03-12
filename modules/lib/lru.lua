-- LRU is a module dedicated for cache, rely on Least Recently used
-- see documentation here : https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_recently_used_(LRU)
-- this is used to limit size of cache, with a limited array of cached key-values. least recently used is removed when try to store a new one after reach size limit.


local lru_cache = {}

-- size 
-- oldest
-- newest 
-- values must be keep always
lru_cache.__newest = 0
lru_cache.__oldest = 1
lru_cache.__max_size = 25000
lru_cache.__listing_num = {}

-- classic calls
function lru_cache.generateLRU(key, value)
  local now = os.time()
  -- maybe copy #lru_cache.__listing_num in a separate var for performance
  if #lru_cache.__listing_num == lru_cache.__max_size then
    -- remove oversize cache value, oldest value
    lru_cache[lru_cache.__listing_num[lru_cache.__oldest]] = nil
    -- change new value
    lru_cache.__listing_num[lru_cache.__oldest] = key
    lru_cache[key] = {
      ex = now + 30, -- Expires
      value = value
    }
    if lru_cache.__oldest == lru_cache.__max_size then
      lru_cache.__newest = lru_cache.__newest + 1
      lru_cache.__oldest = 1
    elseif lru_cache.__newest == lru_cache.__max_size then
      lru_cache.__oldest = lru_cache.__oldest + 1
      lru_cache.__newest = 1
    else
      lru_cache.__newest = lru_cache.__oldest
      lru_cache.__oldest = lru_cache.__oldest + 1
    end
  else
    lru_cache.__newest = lru_cache.__newest + 1
    lru_cache.__listing_num[lru_cache.__newest] = key
    lru_cache[key] = {
      ex = now + 30, -- Expires
      value = value
    }
  end
end
-- custom logic for lru
function lru_cache.get(key, getter, timeout)
  local now = os.time()
  if not lru_cache[key] or lru_cache[key].ex < now then
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
      lru_cache[key] = nil
      return nil
    end
    lru_cache.generateLRU(key, value)
  end
  return lru_cache[key].value
end

function lru_cache.set(key, value, timeout, setter)
  if lru_cache[key] == value then
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
    lru_cache.generateLRU(key, value)
  return value
end

return lru_cache
