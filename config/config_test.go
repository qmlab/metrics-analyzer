package config

import "testing"
import "github.com/stretchr/testify/assert"

func TestConfigLoad(t *testing.T) {
	c := NewConfig(".", OneBox)
	assert.True(t, c.DB.Username == "spiderman")
	assert.True(t, c.DB.Password == "save_the_world")
	assert.True(t, c.MP.APIKey == "key123")
	assert.True(t, c.MP.APIToken == "tokenABC")
}
