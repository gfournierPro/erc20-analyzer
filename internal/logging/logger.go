package logging

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func Init(env, service string) {
	zerolog.TimeFieldFormat = time.RFC3339Nano

	var logger zerolog.Logger
	if env == "dev" {
		logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
			With().Timestamp().Str("service", service).Logger()
	} else {
		logger = zerolog.New(os.Stdout).
			With().Timestamp().Str("service", service).Logger()
	}

	log.Logger = logger
}
