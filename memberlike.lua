--[[
-- ========================================================================= --
-- MEMBERLIKE
--
-- This lua script handles all REDIS actions necessary to record a member's
-- (actor) interest in another member's (actee) attributes in RECON.
--
-- DESCRIPTION
--  The role of this function is to iterate over the given attributes and
--  perform the following:
--
--    1. Increment the given actor's interest frequency in an attribute.
--
--    2. Normalize the actor's interests and update the sorted set accordingly.
--
-- USAGE
--  EVALSHA <SHA> # actor_member_id value [actee_attribute ...]
--
-- EXAMPLE
--  EVALSHA <SHA> 4 131523112 1.0 orientation:straight gender:f ethnicity:white
-- ========================================================================= --
]]--
-- The given member
local this_member_id = KEYS[1]
local this_value = KEYS[2]

-- This key represents a hash of the member's interests, namespaced "hmi"
local member_interests_key = ('hmi:' .. this_member_id)

-- Increment our interest in each attribute by 1
for ii = 3, #KEYS do
  local this_attribute = KEYS[ii]
  redis.call('HINCRBYFLOAT', member_interests_key, this_attribute, this_value)
end

-- Loop over all of the member's attributes recalculating interest
local attrs = redis.call('HGETALL', member_interests_key)
local d = 0
for ii = 1, #attrs, 2 do
  d = (d + attrs[ii + 1])
end
if 0 ~= d then
  for ii = 1, #attrs, 2 do
    local this_attribute_key = ('zmi:' .. attrs[ii])
    redis.call('ZADD', this_attribute_key, (attrs[ii + 1] / d), this_member_id)
  end
end

