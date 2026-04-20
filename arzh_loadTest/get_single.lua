local maxId = 6000
local method = "GET"
local path = "/v0/entity?id="
local headers = { ["Accept"] = "application/json" }

math.randomseed(os.time())

request = function()
    local id = math.random(10, maxId)
    return wrk.format(method, path .. id, headers, nil)
end
