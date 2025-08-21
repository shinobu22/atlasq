package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var l *zap.Logger

func Init(service string) error {
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "@timestamp"
	encCfg.MessageKey = "message"
	encCfg.LevelKey = "level"
	enc := zapcore.NewJSONEncoder(encCfg)

	core := zapcore.NewCore(enc, zapcore.AddSync(os.Stdout), zapcore.InfoLevel)
	base := zap.New(core).With(
		zap.String("service", service),
	)

	l = base
	zap.ReplaceGlobals(l)
	return nil
}

func L() *zap.Logger {
	if l == nil {
		_ = Init("atlasq")
	}
	return l
}

func WithJob(jobID, queue, source string) *zap.Logger {
	return L().With(
		zap.String("job_id", jobID),
		zap.String("queue", queue),
		zap.String("source", source),
	)
}
