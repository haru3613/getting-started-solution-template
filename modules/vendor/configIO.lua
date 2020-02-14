-- This module handle 'configIO' metadata generation for Exosense applications
-- Define here the device data schema as describe in https://github.com/exosite/industrial_iot_schema
-- Limitation: Exosense currently cannot modify per device schema
--
-- This file is in the 'vendor' safeNamespace and changes will persists upon template updates

local config_io = [[
{
  "channels": {
    "machine_status": {
      "display_name": "Machine Status",
      "description": "Device reported status",
      "properties": {
        "data_type": "STRING",
        "primitive_type": "STRING"
      }
    },
    "temperature": {
      "display_name": "Temperature",
      "description": "Temperature Sensor Reading",
      "properties": {
        "data_type": "TEMPERATURE",
        "primitive_type": "NUMERIC",
        "data_unit": "DEG_CELSIUS"
      }
    },
    "button_push": {
      "display_name": "Button On/off",
      "description": "Activate or not",
      "properties": {
        "data_type": "BOOLEAN",
        "control": true
      }
    }
  }
}
]]

return {
  set_to_device = false, -- If set to true, the config_io will be saved & duplicated in each device state
  -- This enables to define a different data schema per device.
  -- By default (false) the above config IO is injected on the fly to the application
  timestamp = 1563939215, -- Unix timestamp of this schema version update
  config_io = config_io
}
