local identities = Device2.listIdentities(operation)
if identities.error or not next(identities.devices) then return identities end

local configIO = require("configIO")
for k, identity in pairs(identities.devices) do
  identity.state = configIO.setState(identity.state);
end
local transform = require("vendor.transform")
if transform and transform.convertIdentityState then
  for k, identity in pairs(identities.devices) do
    -- transform when we have state value
    if identity.state then
      -- flatten state values
      local state = {}
      for k, v in pairs(identity.state) do
        state[k] = v.reported or v
      end
      -- convert state to customized values
      state = transform.convertIdentityState(state)
      -- change back to original format
      for k, v in pairs(state) do
        -- use parsed value to overwrite state values
        if identity.state[k] and identity.state[k].reported then
          identity.state[k] = {
            reported = v,
            set = v,
            timestamp = identity.state[k].timestamp or os.time(os.date("!*t")) * 1000000
          }
        else
          identity.state[k] = v
        end
      end
    end
  end
end

return identities
