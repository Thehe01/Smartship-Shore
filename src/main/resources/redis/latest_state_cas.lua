-- P2-3 latest-state compare-and-set. Atomic by construction: the whole
-- read-compare-write runs inside this single script.
--
-- KEYS[1] : state hash key, e.g. ship:{mmsi}:latest:{type}
-- ARGV[1] : incoming envelope JSON (stored verbatim as the `payload` field)
-- ARGV[2] : incoming event-time epoch millis, or '' when the envelope has none
-- ARGV[3] : incoming sent_at epoch millis, or '' when absent
-- ARGV[4] : incoming msg_id (deterministic Edge fingerprint)
-- ARGV[5] : TTL seconds, refreshed only on a real update
--
-- Hash fields: payload / ts_ms / sent_ms / msg_id.
--
-- Returns 'UPDATED' or 'STALE'.
local function num(s)
  if s == nil or s == '' then
    return nil
  end
  return tonumber(s)
end

local function missing(v)
  if v == nil then
    return -1
  end
  return v
end

local cur = redis.call('HGETALL', KEYS[1])
local h = {}
for i = 1, #cur, 2 do
  h[cur[i]] = cur[i + 1]
end

local cts = num(h['ts_ms'])
local css = num(h['sent_ms'])
local cmid = h['msg_id']

local its = num(ARGV[2])
local iss = num(ARGV[3])
local imid = ARGV[4]

local function write()
  redis.call('HSET', KEYS[1],
    'payload', ARGV[1],
    'ts_ms', ARGV[2],
    'sent_ms', ARGV[3],
    'msg_id', ARGV[4])
  redis.call('EXPIRE', KEYS[1], ARGV[5])
  return 'UPDATED'
end

-- No current state: first observation always wins a slot.
if cmid == nil then
  return write()
end
-- A real event time beats an unknown one, and never the reverse.
if its ~= nil and cts == nil then
  return write()
end
if its == nil and cts ~= nil then
  return 'STALE'
end
-- Both sides carry an event time: strictly newer wins.
if its ~= nil and cts ~= nil then
  if its > cts then
    return write()
  end
  if its < cts then
    return 'STALE'
  end
end
-- Equal event times (including both absent): same msg_id is an idempotent
-- rewrite; otherwise the newer sent_at wins; then the greater msg_id wins.
-- Fully deterministic — no random last-writer behavior.
if imid == cmid then
  return write()
end
local s_i = missing(iss)
local s_c = missing(css)
if s_i > s_c then
  return write()
end
if s_i < s_c then
  return 'STALE'
end
if imid >= cmid then
  return write()
end
return 'STALE'
