--[[
-- ========================================================================= --
-- MEMBERADD
--
-- This lua script handles all REDIS actions necessary to add a member to
-- RECON.
--
-- DESCRIPTION
--  All keys present in the argument list represent attributes of the given
--  member to be added. These include the following:
--
--    - Self-identified attributes (e.g. gender, ethnicity, etc.)
--
--    - Latent Attributes (e.g. subjects discussed, literacy, etc.)
--
--    - Computed Attributes (e.g. attractiveness)
--
--  The role of this function is to iterate over the given attributes and
--  perform the following:
--
--    1. Ensure each attribute is a member of the given member's attribute hash
--       such that, when another member shows interest in him or her, his or
--       her attributes can be retrieved and recorded as preferences of the
--       interested member.
--
--    2. Ensure the given member's identifier exists in the attribute set for
--       each attribute associated in his or her profile. This is used for
--       bitwise-AND type query pruning.
--
-- USAGE
--  EVALSHA <SHA> # member_id [attribute ...]
--
-- EXAMPLE
--  EVALSHA <SHA> 4 131523112 orientation:straight gender:f ethnicity:white
-- ========================================================================= --
]]--

-- The given member
local this_member_id = KEYS[1]

-- This key represents a hash of the member's attributes, namespaced "hma"
local member_attributes_key = ('hma:' .. this_member_id)

-- Loop over each attribute given
for ii = 2, #KEYS do
  local this_attribute_key = KEYS[ii]

  --[[
  -- This key represents a sorted set of members with the given attribute,
  -- namespaced "zma"
  ]]--
  local this_attribute_zkey = ('zma:' .. this_attribute_key)

  -- Add the attribute to this member's attribute hash.
  redis.call('HSET', member_attributes_key, this_attribute_key, 1)

  -- Add the member identifier to the sorted set.
  redis.call('ZADD', this_attribute_zkey, 1, this_member_id)
end
