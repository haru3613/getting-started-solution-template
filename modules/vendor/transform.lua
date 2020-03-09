local transform = {}

-- local parser_factory = require("bytes.parser_factory")
-- function transform.DecodeMode(uplink)
--   -- Here decode data from uplink string (the 12 bytes transmitted by devices)
--   -- Generated data in 'data_in' must be defined in the module 'vendor.configIO'
--   -- In this example an integer in first byte is used as mode
--   local mode = parser_factory.getuint(parser_factory.fromhex(uplink),0,8)
--   if mode == 1 then
--     -- Here decode the remaining bytes base on the mode. Example: temperature as float
--     return {temperature = parser_factory.getfloat_32(parser_factory.fromhex(uplink),1)}
--   elseif mode == 2 then
--     -- Another mode example extracting a Boolean value
--     return {button_enabled = parser_factory.getbool(parser_factory.fromhex(uplink),1,2)}    
--   else
--     log.error("Un-supported mode " .. mode .. " from uplink: " .. uplink)
--     return {}
--   end
-- end


-- function convertState(data_in_source)
--   local data_in, err = json.parse(data_in_source)
--   if err ~= nil then
--     return data_in_source
--   end

--   if data_in.gps ~= nil then
--     if data_in.gps.lat ~= nil then
--       data_in.gps.lat = data_in.gps.lat / 1000000
--     end
--     if data_in.gps.lng ~= nil then
--       data_in.gps.lng = data_in.gps.lng / 1000000
--     end
--   end

--    In this example we generate dynamically a new channel 'sum_ab'
--    This virtual custom channel MUST be defined in the module 'vendor.configIO'
--    if type(data_in.a) == "number" and type(data_in.b) == "number" then
--      data_in.sum_ab = data_in.a + data_in.b
--    end
--   return json.stringify(data_in)
-- end

-- function transform.decodeMode(uplink)
--   local mode = parser_factory.getuint(parser_factory.fromhex(uplink),0,8)
--   if mode == 4 then
--     return "motion"
--   elseif mode == 0 then
--     return "timer"
--   else
--     return "unknown"
--   end
-- end

-- function transform.convertIdentityState(state)
--   if state == nil then return state end

--   if state.uplink ~= nil then
--     if state.data_in == nil then state.data_in = {} end
--     -- Parse first byte and save it to data_in
--     state.data_in.status_mode = transform.decodeMode(state.uplink)
--     -- Parse more bytes
--     -- ...
--     -- ...
--   end

--   if state.data_in and type(state.data_in) ~= "string" then
--     state.data_in = to_json(state.data_in)
--   end

--   return state
-- end

return transform
