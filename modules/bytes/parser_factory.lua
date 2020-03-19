--Help Senseway uplink_decoding in tranform, who takes raw hex data and parsed it for Murano, and also downlink_encoding
--see also https://docs.exosite.com/development/services/sigfox/decoding-grammar/
local parser_factory = {}

function parser_factory.getbool(bytes, start, offset)
  return 1 == bit.band(bit.rshift(string.byte(bytes,start+1),offset),1)
end

function parser_factory.getstring(bytes, start, size, offset)
  offset = offset or 0
  local my_int_string = ''
  for i = 1, size do
  -- offset to do
    my_int_string = my_int_string..string.char(string.byte(bytes,start+i))
  end
  return my_int_string

end

function parser_factory.getuint(bytes, start, size, offset)
  -- should add https://stackoverflow.com/questions/40943556/how-can-i-convert-a-pair-of-bytes-into-a-signed-16-bit-integer-using-lua for sign issue
  -- create a size/8 len loop to iterate over bytes and adding each byte one after one
  -- return generated number
  offset = offset or 0
  -- offset to do
  local my_int = 0
  for i = 1, math.floor(size*1.0/8) do
    --could add some limit with modulo
    my_int = my_int + math.pow(256, i-1) * string.byte(bytes, math.floor(size*1.0/8)+1-i + start)
  end
  return my_int
end

function parser_factory.getint(bytes, start, size, offset)
  offset = offset or 0
  -- offset to do
  local my_int = 0
  local most_signif  = 0
  if string.byte(bytes, start + 1) > 127 then most_signif = -1 * math.pow(2,size-1) end
  -- set offset , will generate 
  for i = 1, math.floor(size*1.0/8) do
    if i == math.floor(size*1.0/8) then
      my_int = my_int + math.pow(256, i-1) *(0 + string.byte(bytes, 1 + start)%(math.pow(2,((size*1.0%8)+7)%8)))
    else
      my_int = my_int + math.pow(256, i-1) *(0 + string.byte(bytes, math.floor(size*1.0/8)+1-i + start))
    end
  end
  return my_int + most_signif
end


function parser_factory.getfloat_32(bytes, start, offset)
  offset = offset or 0
  -- offset to do
  --http://lua-users.org/lists/lua-l/2010-03/msg00910.html
  local sign = 1
  local mantissa = string.byte(bytes, start + 2) % 128
  for i = 3, 4, 1 do mantissa = mantissa * 256 + string.byte(bytes, start + i) end
  if string.byte(bytes, start + 1) > 127 then sign = -1 end
  local exponent = (string.byte(bytes, start + 1) % 128) * 2 +
                   math.floor(string.byte(bytes, start + 2) / 128)
  if exponent == 0 then return 0 end
  mantissa = (math.ldexp(mantissa, -23) + 1) * sign
  return math.ldexp(mantissa, exponent - 127)
end

function parser_factory.getfloat_64(bytes, start, offset)
  offset = offset or 0
  -- offset to do
  -- https://stackoverflow.com/questions/9168049/parsing-ieee754-double-precision-floats-in-pure-lua code solution, adapted
  local b1, b2, b3, b4, b5, b6, b7, b8 = string.byte(bytes, start+1, start+8)
  --Separate out the values
  local sign = b1 >= 128 and 1 or 0
  local exponent = (b1%128)*16 + math.floor(b2/16)
  local fraction = (b2%16)*math.pow(2, 48) 
                   + b3*math.pow(2, 40) + b4*math.pow(2, 32) + b5*math.pow(2, 24)
                   + b6*math.pow(2, 16) + b7*math.pow(2, 8) + b8
  --Handle special cases
  if (exponent == 2047) then
      --Infinities
      if (fraction == 0) then return math.pow(-1,sign) * math.huge end

      --NaN
      if (fraction == math.pow(2,52)-1) then return 0/0 end
  end
  --Combine the values and return the result
  if (exponent == 0) then
      --Handle subnormal numbers
      return math.pow(-1,sign) * math.pow(2,exponent-1023) * (fraction/math.pow(2,52))
  else
      --Handle normal numbers
      return math.pow(-1,sign) * math.pow(2,exponent-1023) * (fraction/math.pow(2,52) + 1)
  end
end


-- Aux functions 
-- 2 following are from github  https://gist.github.com/yi/01e3ab762838d567e65d
function parser_factory.fromhex(str)
  return (str:gsub('..', function (cc)
      return string.char(tonumber(cc, 16))
  end))
end
function parser_factory.tohex(str)
  return (str:gsub('.', function (c)
      return string.format('%02X', string.byte(c))
  end))
end

function parser_factory.sendbool(mess)
  if mess == "true" then
    return "00000001"
  else
    return "00000000"
  end
end

function check_for_error(len_candidate, len_actual, len_available, object_message)
  -- this function fill error field of object_mlessage if there is not enough bytes availables.
  if len_actual + len_candidate > len_available then
    object_message.error = 'Insufficient bytes available for your definitions'
    return true
  end
  return false
end

function parser_factory.get_data_type(def)
  -- this function will detect type of definition and associated parameters for unpack some bytes. return Table
  local r = {}
  -- NOT OPTIMIZED , should be refractored to something simple with correct regex
  for v in string.gmatch(def,":%a+") do
    r.name = string.sub(v,2)
  end
  for v in string.gmatch(def,":%d+") do 
    r.size = string.sub(v,2)
    r.offset = string.sub(v,2)
  end
  for v in string.gmatch(def,"%d+:") do 
    r.byte_offset = string.sub(v,0,-2)
  end
  if not r.name then
    r.error = true
  end
  return r
end

function parser_factory.switch_resource(ready_payload, final_mess, index, tag_filter, message)
  --dispatcher
  if ready_payload.byte_offset~=nil then
    print('add byte offset')
    index = index + ready_payload.byte_offset
  end
  if ready_payload.name == 'uint' then
    if check_for_error(math.ceil(ready_payload.size/8), index, #message, final_mess) then
      return index
    end
    final_mess[tag_filter.resource] = parser_factory.getuint(message,index,ready_payload.size)
    --add size offset
    index = index + math.ceil(ready_payload.size/8)
  elseif ready_payload.name == 'int' then
    if check_for_error(math.ceil(ready_payload.size/8), index, #message, final_mess) then
      return index
    end
    final_mess[tag_filter.resource] = parser_factory.getint(message,index,ready_payload.size)
    --add size offset
    index = index + math.ceil(ready_payload.size/8)
  elseif ready_payload.name == 'bool' then
    if check_for_error(1, index, #message, final_mess) then
      return index
    end
    final_mess[tag_filter.resource] = parser_factory.getbool(message,index,ready_payload.offset)
  elseif ready_payload.name == 'char' then
    if check_for_error(ready_payload.size, index, #message, final_mess) then
      return index
    end
    final_mess[tag_filter.resource] = parser_factory.getstring(message,index,ready_payload.size)
    --add size offset
    index = index + ready_payload.size
  elseif ready_payload.name == 'float' then
    if ready_payload.size == '32' then
      if check_for_error(4, index, #message, final_mess) then
        return index
      end
      final_mess[tag_filter.resource] = parser_factory.getfloat_32(message,index)
      index = index + 4
    else
      if check_for_error(8, index, #message, final_mess) then
        return index
      end
      final_mess[tag_filter.resource] = parser_factory.getfloat_64(message,index)
      --add size more offset
      index = index + 8
    end

  elseif ready_payload.error then
    final_mess["error"] = 'Resource misspelled'
  end
  return index
end

-- main function 
function parser_factory.parse_payloads(dict, message)
--arugments:
--     Array: read different custom paylaods 
--           format [{ 
--                    - resource,
--                    - definition 
--                  },...]
--     message: message in hex to be send, will be converted in ascii decim
-- The parse_payload will detect any error like resource mispelled or Insufficient bytes compared to all resources needed
-- in a dedicated in error field for final_message.
  --converted in Decim
  message = parser_factory.fromhex(message)
  --cursor, index where reading message
  local cursor = 0
  local final_message = {}
  --raw elem from payloads array is tag_filter with def and resource.
  for _index, tag_filter in ipairs(dict) do
    local ready_payload = parser_factory.get_data_type(tag_filter.definition)
    --for each ready payload, can switch to detect which operation to do, and move cursor.
    cursor = parser_factory.switch_resource(ready_payload, final_message, cursor, tag_filter, message)
  end 
  return final_message
end

--payloads from https://docs-staging.exosite.com/development/services/sigfox/decoding-grammar/
-- exemple1
res1 = {["resource"]= "bool1", ["definition"] = ":bool:2"}
res2 = {["resource"]= "uint1", ["definition"] = ":uint:16"}
res3 = {["resource"]= "uint2", ["definition"] = ":uint:16"}
res4 = {["resource"] = "float1", ["definition"] = ":float:32"}
local payloads = 
{
  res1,res2,res3,res4
}
local response = parser_factory.parse_payloads(payloads,"C1234015405C8254")

return parser_factory
