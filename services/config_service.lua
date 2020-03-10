
local murano2cloud = require("c2c.murano2cloud")

-- This event listen to the changes made on the mqtt to change automatically initial dedicated topic
if service.service == "mqtt" and service.action == "updated" then
  local result = Config.getParameters({service = service.service})
  if(result.parameters.topics and #(result.parameters.topics)>0) then
    return
  else
    local topic_user = {}
    topic_user[1] = "devices/+/uplink"
    topic_user[2] = "devices/+/downlink"
    Config.setParameters({service = service.service, parameters = { topics = topic_user }})
  end
end