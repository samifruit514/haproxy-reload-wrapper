ifndef VERSION 
	override VERSION = $(shell git rev-parse --short HEAD)
endif
