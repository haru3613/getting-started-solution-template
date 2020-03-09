local identity = Device2.getIdentity(operation)
if identity.error then return identity end

local configIO = require("configIO")
identity.state = configIO.setState(identity.state);

local transform = require("vendor.transform")
if transform and transform.convertIdentityState then
  -- flatten values
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

return identity
