-- This file enable device data transformation for the template user
-- MODIFY IT TO SUIT YOUR NEEDS.
-- 
-- You MUST define here how to decode your device Hex values into a standard type for application ingestion.
-- All transformations MUST match channels defined in the module `vendor.configIO` , or associated with each device if used by exosense
--
-- This file is in the 'vendor' safeNamespace and changes will persists upon template updates
local transform = {}

-- local parser_factory = require("bytes.parser_factory")

-- -- Defines a decoding function
-- local function decode_temp_status(hex)
--    -- Here one port data contains 2 channels values, so a map needs to be returned:
--    return to_json({
--       -- Data attribute temperature must match with configIO
--       ["temperature"] = parser_factory.getfloat_32(parser_factory.fromhex(hex),0),
--       ["machine_status"] = parser_factory.getstring(parser_factory.fromhex(hex),4,5)
--     })
-- end

-- local uplink_decoding = {
--   -- keys MUST match the channels name defined in the module `vendor.configIO` 
--   ["temperature"] = decode_temp_status,
--   ["machine_status"] = decode_temp_status
--   -- Other Cases for other channels must be implemented here
-- }

-- -- Here downling channels will be transformed to expected Hex value
-- -- On config IO, corresponding to channel(s) MUST defines `properties.control` to `true`
-- local downlink_encoding = {
--   ["button_push"] = function(new_machine_status) 
--     return {
--       ["cnf"] = false,
--       ["data"] = parser_factory.sendbool(tostring(new_machine_status))
--       -- port is set automatically from the configIO `protocol_config.app_specific_config.port` value
--     }
--     end
--   -- Other Cases for other ports must be implemented
-- }

-- function transform.data_in(cloud_data)
--  -- Transform data from the 3rd party service to Murano
--  if uplink_decoding[cloud_data.channel] ~= nil then
--    return uplink_decoding[cloud_data.channel](cloud_data.mod.data)
--  else
--    return '{}'
--  end
-- end

-- function transform.data_out(murano_data)
--   if murano_data ~= nil then
--     -- Transform data from Murano to the 3rd party service : hex message sent to the device.
--     -- ConfigIO `protocol_config.app_specific_config.port` channels values is used to match murano_data downlink keys into lorawan port
--     for key, value in pairs(murano_data) do
--       if downlink_encoding[key] ~= nil then
--         return downlink_encoding[key](value)
--       else
--         return nil
--       end
--     end
--   end
--   return nil
-- end

return transform
