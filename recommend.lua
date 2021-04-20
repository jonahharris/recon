--[[
-- ========================================================================= --
-- RECOMMEND
--
-- This script calculates and returns the top-n most compatible matches for
-- a given member.
--
-- DESCRIPTION
--  This function computes all compatibility scores for a given member and
--  returns the top-n most compatible by performing the following:
--
--    1. Build a sorted set containing all members who have attributes defined
--       in the disjunct (OR) key list with an assigned score of 0. A common
--       disjunction example is all ages in a queried age range range (e.g.
--       for a filter of age [19, 21], we'd OR age:19, age:20, and age:21)
--
--    2. Build a sorted set containing all members who meet a hard filter
--       by intersecting our sorted set of disjunctions with all sorted sets
--       defined in the conjunct (AND) key list with a score of 0. Common
--       conjunctions are mutually-exclusive attributes (e.g. gender:female,
--       orientation:straight, and body_type:athletic).
--
--    3. Build a sorted set from all members to me using the aggregate sum
--       of their scores for each of my attributes. This represents a one-way
--       score for each member's interest in me.
--
--    4. Build a sorted set from me to all members who have attributes I've
--       shown interest in by summing the sorted set for each attribute and
--       using the weights I've calculated.
--
--    5. Build a sorted set containing all possible matches by intersecting
--       the two one-way sorted sets built in steps 3 and 4 as well as the
--       sorted set resulting from step 2 (to apply hard filters) using a
--       summation aggregate.
--
--    6. Build a sorted set containing the computed reciprocal compatibility
--       scores by iterating over the sorted set created in step 5 and, for
--       each member, perform the following:
--
--        a. Retrieve the member's score from one of the one-way sets.
--
--        b. Subtract that score from the summation to identify the other
--           member's score.
--
--        c. Perform a harmonic mean calculation of the two scores.
--
--        d. Add the resulting calculation to a new sorted set.
--
--    7. Return the top-n most compatible members based on descending score
--       values of the sorted set created in step 6.
--
-- USAGE
--  EVALSHA <SHA> <# OF KEYS> 
--    <# OF MEMBERS TO RECOMMEND>
--    <# OF OR FILTER ATTRIBUTES> [OR FILTER ATTRIBUTES]
--    <# OF AND FILTER ATTRIBUTES> [AND FILTER ATTRIBUTES]
--    <# OF MY ATTRIBUTES> [MY ATTRIBUTES]
--    <# OF MY INTERESTS> [<MY INTEREST> <MY WEIGHT>]
--
-- EXAMPLE (Top 10)
--  EVALSHA <SHA> <#>
--    10
--    3 age:23 age:26 age:30
--    1 gender:female
--    15 tags:33 tags:18 tags:38 tags:25 tags:31 looking_for:friendship
--      looking_for:dating looking_for:chat orientation:straight
--      body_type:about_average has_children:no gender:m
--      education:bachelors_degree ethnicity:white_caucasian age:34
--    6 gender:female 33 age:23 3 age:26 13
--      age:30 13 body:slim 3 body:average 13
-- ========================================================================= --
]]--

--[[
-- Parse the passed-in keys and validate proper usage of this function using
-- a simple state machine and counter register for looping.
]]--
local parse_cardinality_state = 1
local parse_disjunction_count_state = 2
local parse_disjunction_state = 3
local parse_conjunction_count_state = 4
local parse_conjunction_state = 5
local parse_attributes_count_state = 6
local parse_attributes_state = 7
local parse_interests_count_state = 8
local parse_interests_name_state = 9
local parse_interests_weight_state = 10
local parse_complete_state = 11
local parsed_keys = {
  cardinality   = 0,
  disjunctions  = {},
  conjunctions  = {},
  attributes    = {},
  interests     = {},
}
local current_state = parse_cardinality_state
local counter = 0
for ii = 1, #KEYS do
  if current_state == parse_cardinality_state then
    parsed_keys['cardinality'] = tonumber(KEYS[ii])
    current_state = parse_disjunction_count_state
  elseif current_state == parse_disjunction_count_state then
    counter = tonumber(KEYS[ii])
    current_state = parse_disjunction_state
  elseif current_state == parse_disjunction_state then
    table.insert(parsed_keys['disjunctions'], ('zma:' .. KEYS[ii]))
    counter = (counter - 1)
    if (0 == counter) then
      current_state = parse_conjunction_count_state
    end
  elseif current_state == parse_conjunction_count_state then
    counter = tonumber(KEYS[ii])
    current_state = parse_conjunction_state
  elseif current_state == parse_conjunction_state then
    table.insert(parsed_keys['conjunctions'], ('zma:' .. KEYS[ii]))
    counter = (counter - 1)
    if (0 == counter) then
      current_state = parse_attributes_count_state
    end
  elseif current_state == parse_attributes_count_state then
    counter = tonumber(KEYS[ii])
    current_state = parse_attributes_state
  elseif current_state == parse_attributes_state then
    table.insert(parsed_keys['attributes'], ('zmi:' .. KEYS[ii]))
    counter = (counter - 1)
    if (0 == counter) then
      current_state = parse_interests_count_state
    end
  elseif current_state == parse_interests_count_state then
    counter = tonumber(KEYS[ii])
    current_state = parse_interests_name_state
  elseif current_state == parse_interests_name_state then
    current_state = parse_interests_weight_state
  elseif current_state == parse_interests_weight_state then
    parsed_keys['interests'][('zma:' .. KEYS[ii - 1])] = tonumber(KEYS[ii])
    counter = (counter - 1)
    if (0 == counter) then
      current_state = parse_complete_state
    else
      current_state = parse_interests_name_state
    end
  end
end
if current_state ~= parse_complete_state then
  redis.log(redis.LOG_NOTICE, 'PARSE FAILED')
else
  redis.log(redis.LOG_NOTICE, 'PARSE COMPLETE')
end

-- ========================================================================= --
-- STEP 1 ------------------------------------------------------------------ --
-- ========================================================================= --

local step_one_key = 'zs1';
redis.log(redis.LOG_NOTICE, 'zunionstore', step_one_key,
  #parsed_keys['disjunctions'], unpack(parsed_keys['disjunctions']))
local c = redis.call('zunionstore', step_one_key,
  #parsed_keys['disjunctions'], unpack(parsed_keys['disjunctions']))
if 0 == c then
  redis.log(redis.LOG_NOTICE, 'Step 1 Failed')
end

-- ========================================================================= --
-- STEP 2 ------------------------------------------------------------------ --
-- ========================================================================= --

local step_two_key = 'zs2'
local step_two_args = {}
local step_two_args_length = 0
table.insert(step_two_args, step_one_key)
for ii = 1, #parsed_keys['conjunctions'] do
  table.insert(step_two_args, parsed_keys['conjunctions'][ii])
end
step_two_args_length = #step_two_args
table.insert(step_two_args, 'weights')
for ii = 1, step_two_args_length do
  table.insert(step_two_args, 0)
end
redis.log(redis.LOG_NOTICE, 'zinterstore', step_two_key,
  step_two_args_length, unpack(step_two_args))
local c = redis.call('zinterstore', step_two_key,
  step_two_args_length, unpack(step_two_args))
redis.log(redis.LOG_NOTICE, c)
if 0 == c then
  redis.log(redis.LOG_NOTICE, 'Step 2 Failed')
end

-- ========================================================================= --
-- STEP 3 ------------------------------------------------------------------ --
-- ========================================================================= --

local step_three_key = 'zs3';
redis.log(redis.LOG_NOTICE, 'zunionstore', step_three_key,
  #parsed_keys['attributes'], unpack(parsed_keys['attributes']))
local c = redis.call('zunionstore', step_three_key,
  #parsed_keys['attributes'], unpack(parsed_keys['attributes']))
redis.log(redis.LOG_NOTICE, c)
if 0 == c then
  redis.log(redis.LOG_NOTICE, 'Step 3 Failed')
end

-- ========================================================================= --
-- STEP 4 ------------------------------------------------------------------ --
-- ========================================================================= --

local step_four_key = 'zs4'
local step_four_args = {}
local step_four_args_length = 0
for k, v in pairs(parsed_keys['interests']) do
  table.insert(step_four_args, k)
end
step_four_args_length = #step_four_args
table.insert(step_four_args, 'weights')
for k, v in pairs(parsed_keys['interests']) do
  table.insert(step_four_args, v)
end
redis.log(redis.LOG_NOTICE, 'zunionstore', step_four_key,
  step_four_args_length, unpack(step_four_args))
local c = redis.call('zunionstore', step_four_key,
  step_four_args_length, unpack(step_four_args))
redis.log(redis.LOG_NOTICE, c)
if 0 == c then
  redis.log(redis.LOG_NOTICE, 'Step 4 Failed')
end

-- ========================================================================= --
-- STEP 5 ------------------------------------------------------------------ --
-- ========================================================================= --

local step_five_key = 'zs5'
redis.log(redis.LOG_NOTICE, 'zinterstore', step_five_key, 3,
  step_two_key, step_three_key, step_four_key)
local c = redis.call('zinterstore', step_five_key, 3,
  step_two_key, step_three_key, step_four_key)
redis.log(redis.LOG_NOTICE, c)
if 0 == c then
  redis.log(redis.LOG_NOTICE, 'Step 5 Failed')
end

-- ========================================================================= --
-- STEP 6 ------------------------------------------------------------------ --
-- ========================================================================= --

local step_six_key = 'zs6'
local members = redis.call('zrange', step_five_key, 0, -1, 'withscores')
for ii = 1, #members, 2 do
  local member_id = members[ii]
  local total_score = members[(ii + 1)]
  redis.log(redis.LOG_NOTICE, 'Member', member_id, 'Total', total_score)
  local score_one = redis.call('ZSCORE', step_four_key, member_id)
  local score_two = (total_score - score_one)
  local rscore = (2.0 / (math.pow(score_one, -1) + math.pow(score_two, -1)))
  redis.log(redis.LOG_NOTICE, 's1', score_one, 's2', score_two, 'rscore', rscore)
  if rscore > 0.01 then
    redis.call('zadd', step_six_key, rscore, member_id)
  end
end

-- ========================================================================= --
-- STEP 7 ------------------------------------------------------------------ --
-- ========================================================================= --

local s = redis.call('zrevrangebyscore', step_six_key, '+inf', '0',
  'withscores', 'limit', 0, parsed_keys['cardinality'])
--for i = 1, #s, 2 do
  --redis.call('ZADD', 'recipufinal', s[i + 1], s[i])
--end
return s
