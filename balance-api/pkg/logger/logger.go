package logger

import (
	"os"

	"github.com/rs/zerolog"
)

func InstanceWithLevel(level zerolog.Level) zerolog.Logger {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	return zerolog.New(os.Stderr).Level(level).With().Timestamp().Logger()
}

func Instance() zerolog.Logger {
	return InstanceWithLevel(zerolog.InfoLevel)
}
