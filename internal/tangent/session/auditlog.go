package session

import (
	"context"
	"path/filepath"

	jsonitor "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/tangent/config"
	"github.com/tansive/tansive-internal/internal/tangent/session/hashlog"
)

func GetAuditLogPath(sessionID string) string {
	// get os application data directory
	auditLogDir := config.GetAuditLogDir()
	auditLogPath := filepath.Join(auditLogDir, sessionID+".tlog")
	return auditLogPath
}

func InitAuditLog(ctx context.Context, session *session) {
	auditLogPath := GetAuditLogPath(session.id.String())
	log.Ctx(ctx).Info().Str("audit_log_path", auditLogPath).Msg("initializing audit log")
	session.auditLogger = session.getLogger(TopicAuditLog).With().Str("actor", "system").Logger()

	logWriter, err := hashlog.NewHashLogWriter(auditLogPath, 100)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create audit logger")
	}
	auditLog, unsubAuditLog := GetEventBus().Subscribe(session.getTopic(TopicAuditLog), 100)

	finalizeLog := func() {
		logWriter.Flush()
		logWriter.Close()
		unsubAuditLog()
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Ctx(ctx).Error().Msgf("panic in audit log: %v", r)
			}
			finalizeLog()
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-auditLog:
				if !ok {
					return
				}
				data, ok := event.Data.([]byte)
				if !ok {
					continue
				}
				var logMap map[string]any
				if err := jsonitor.Unmarshal(data, &logMap); err != nil {
					log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal audit log")
					continue
				}
				logWriter.AddEntry(logMap)
			}
		}
	}()
}
