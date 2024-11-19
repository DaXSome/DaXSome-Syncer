package utils

import (
	"log"

	"github.com/fatih/color"
)

func Logger(logType string, stmts ...interface{}) {
	var logger *color.Color

	switch logType {
	case "storage":
		logger = color.New(color.FgHiGreen)
	case "error":
		logger = color.New(color.FgHiRed)
	case "database":
		logger = color.New(color.FgHiMagenta)

	default:
		logger = color.New(color.FgHiBlue)
	}

	l := logger.SprintFunc()

	log.Println(l(stmts...))
}
