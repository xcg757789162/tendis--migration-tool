package main

import (
	"github.com/gin-gonic/gin"
)

func main() {
	r := gin.Default()
	
	tasks := r.Group("/api/v1/tasks")
	{
		tasks.GET("", func(c *gin.Context) { c.String(200, "list") })
		tasks.GET("/:id", func(c *gin.Context) { c.String(200, "get "+c.Param("id")) })
		tasks.GET("/:id/progress", func(c *gin.Context) { c.String(200, "progress "+c.Param("id")) })
		tasks.GET("/:id/conflicts", func(c *gin.Context) { c.String(200, "conflicts "+c.Param("id")) })
		tasks.GET("/:id/conflicts/summary", func(c *gin.Context) { c.String(200, "conflicts summary "+c.Param("id")) })
	}
	
	r.Run(":9199")
}
