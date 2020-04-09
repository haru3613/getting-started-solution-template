-- This file enable device data transformation for the template user
-- MODIFY IT TO SUIT YOUR NEEDS.
-- 
-- You MUST define here how to decode your device Hex values into a standard type for application ingestion.
-- All transformations MUST match channels defined in the module `vendor.configIO` , or associated with each device if used by exosense
--
-- This file is in the 'vendor' safeNamespace and changes will persists upon template updates
local transform = {}

local parser_factory = require("bytes.parser_factory")

-- Defines a decoding function
local function decode_temp_status(cloud_data)
  -- Here temperature and status contains 2 channels values, so a map needs to be returned:
  return to_json({
    -- Data attribute temperature must match with configIO
    ["temperature"] = parser_factory.getfloat_32(parser_factory.fromhex(cloud_data.temperature),0),
    ["machine_status"] = parser_factory.getstring(parser_factory.fromhex(cloud_data.machine_status),0,5)
  })
end

transform.uplink_decoding = {
  -- keys MUST match the last string (EndsWith) of an uplink topic, to route to specific decoding logic.
  ["g1/+/temperature/uplink"] = decode_temp_status
  -- Other Cases for other topics must be implemented here
}

-- Here downling channels will be transformed to expected value read in device (downlink)
-- On config IO, corresponding to channel(s) MUST defines `properties.control` to `true`
local downlink_encoding = {
  ["button_push"] = function(new_machine_status) 
  return {
    -- to generate a encoded message
    ["data"] = parser_factory.sendbool(tostring(new_machine_status))
    -- port is set automatically from the configIO `protocol_config.app_specific_config.port` value
  }
  end
  -- Other Cases for other ports must be implemented
}

function transform.data_in(cloud_data, key)
  -- Transform data from the 3rd party service to Murano, original data should be in data field
  if transform.uplink_decoding[key] ~= nil then
    return transform.uplink_decoding[key](cloud_data)
  else
    return '{}'
  end
end

function transform.data_out(key, value)
  if key ~= nil then
    -- Transform data from Murano to the 3rd party service :  message sent to the device.
    -- your logic depends channel name, which is key here
    if downlink_encoding[key] ~= nil then
      return downlink_encoding[key](value)
    else
      return nil
    end
  end
  return nil
end

return transform
