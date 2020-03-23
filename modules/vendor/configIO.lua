-- This module handle 'configIO' metadata generation for Exosense applications
-- Define & customize here the device data schema matching your devices settings, as described in https://github.com/exosite/industrial_iot_schema
-- This schema will be applied by default to all devices. However, user can update it through ExoSense per device.
-- IMPORTANT: All channels configuration MUST defines the device sensor port number in the attribute `protocol_config.app_specific_config.port`
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
      },
      "protocol_config": {
        "app_specific_config": {
          "port": 1
        }
      }
    },
    "temperature": {
      "display_name": "Temperature",
      "description": "Temperature Sensor Reading",
      "properties": {
        "data_type": "TEMPERATURE",
        "primitive_type": "NUMERIC",
        "data_unit": "DEG_CELSIUS"
      },
      "protocol_config": {
        "app_specific_config": {
          "port": 1
        }
      }
    },
    "button_push": {
      "display_name": "Button On/off",
      "description": "This is an out-going data example.",
      "properties": {
        "data_type": "BOOLEAN",
        "control": true
      },
      "protocol_config": {
        "app_specific_config": {
          "port": 5
        }
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
