local result={ }
for i=1, #KEYS, 1 do
    result[i] = redis.call("llen",KEYS[i])
end
return result
