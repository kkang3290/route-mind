package redis

import (
	_ "embed"
	goredis "github.com/redis/go-redis/v9"
)

//go:embed coordination.lua
var coordinationSource string
var coordinationScript = goredis.NewScript(coordinationSource)
