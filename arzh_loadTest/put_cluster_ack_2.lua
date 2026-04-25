local method = "PUT"
local path = "/v0/entity?id="
local ack = "&ack=2"
local headers = { ["Content-Type"] = "application/json" }
local body = '{"payload": "data"}'
local counter = 0

local targets = {
   "http://localhost:8080",
   "http://localhost:8081",
   "http://localhost:8082"
}

math.randomseed(os.time())

request = function()
   counter = counter + 1
   local host = targets[math.random(#targets)]
   local url = host .. path .. counter .. ack
   return wrk.format(method, url, headers, body)
end
