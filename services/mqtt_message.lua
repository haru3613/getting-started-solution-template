local cloud2murano = require("c2c.cloud2murano")
local options = {
  --add any options, for cache or anything
}
cloud2murano.callback(batch.messages, options)
