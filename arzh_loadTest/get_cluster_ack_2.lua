local maxId = 3000
local method = "GET"
local path = "/v0/entity?id="
local ack = "&ack=2"
local headers = { ["Accept"] = "application/json" }

local targets = {
   "http://localhost:8080",
   "http://localhost:8081",
   "http://localhost:8082"
}

math.randomseed(os.time())

request = function()
    local id = math.random(10, maxId)
    local host = targets[math.random(#targets)]
    local url = host .. path .. id .. ack
    return wrk.format(method, url, headers, nil)
end
