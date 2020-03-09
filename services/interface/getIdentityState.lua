local identityState = Device2.getIdentityState(operation)
if identityState.error then return identityState end

local configIO = require("configIO")
identityState = configIO.setState(identityState);

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
    if identityState[k] and identityState[k].reported then
      identityState[k] = {
        reported = v,
        set = v,
        timestamp = identityState[k].timestamp or os.time(os.date("!*t")) * 1000000
      }
    else
      identityState[k] = v
    end
  end
end

return identityState
