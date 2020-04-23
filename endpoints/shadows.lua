-- WARNING: Do not modify, changes will not persist updates
-- You can safely add new endpoints under /vendor/*

--#ENDPOINT GET /api/shadow/{identity}/get
--#SECURITY none
-- This endpoint publish on shadow topic get so that user can have last state of device.
-- Dont use it if not subscriber of shadows topic ( $aws/things/+/shadow/#)
local r = require("pdaas.lib.router")
local e = require("pdaas.lib.errors")

function PublishMqtt(request)
  local identity = request.parameters.identity
  local message = " "
  -- verify first user configured an $aws shadow subscription
  local my_service = Config.getParameters({service = "mqtt"})
  local uses_aws = false
  if(my_service.parameters.topics and #(my_service.parameters.topics) > 0) then
    for _,topic in pairs(my_service.parameters.topics) do
      if topic == "$aws/things/+/shadow/#" then
        uses_aws = true
        break
      end
    end
  end
  if uses_aws then
    local response = Mqtt.publish({messages={{topic ="$aws/things/" .. identity .. "/shadow/get", message = message}}})
    -- after publish on good topic, verify if it reach broker
    if response.error then
      return nil, e.get(400, "Didn't reach AWS IoT broker")
    end
    return response
  end
  return nil, e.get(400, "Not subscribing to aws shadow, cannot use endpoint")
end

r:start(request):run(PublishMqtt):response(response)
