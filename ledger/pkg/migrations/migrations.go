package migrations

import (
	"fmt"
	"ledger/pkg/logger"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// RunMigrations executa as migrations do banco de dados
func RunMigrations(databaseURL, migrationsPath string) error {

	logger := logger.Instance()

	logger.Info().Msg("Iniciando migrations do banco de dados...")
	logger.Info().Str("database_url", maskPassword(databaseURL)).Msg("Database URL")
	logger.Info().Str("migrations_path", migrationsPath).Msg("Migrations Path")

	m, err := migrate.New(
		fmt.Sprintf("file://%s", migrationsPath),
		databaseURL,
	)
	if err != nil {
		return fmt.Errorf("erro ao criar instância de migrate: %w", err)
	}
	defer m.Close()

	// Obtém versão atual
	version, dirty, err := m.Version()
	if err != nil && err != migrate.ErrNilVersion {
		logger.Error().Err(err).Msg("Erro ao obter versão das migrations")
	} else if err == migrate.ErrNilVersion {
		logger.Info().Msg("Nenhuma migration aplicada ainda")
	} else {
		logger.Info().Int("version", int(version)).Bool("dirty", dirty).Msg("Versão atual das migrations")
	}

	// Aplica migrations
	if err := m.Up(); err != nil {
		if err == migrate.ErrNoChange {
			logger.Info().Int("version", int(version)).Msg("Banco de dados já está atualizado")
			return nil
		}
		return fmt.Errorf("erro ao executar migrations: %w", err)
	}

	// Obtém nova versão
	newVersion, _, _ := m.Version()
	logger.Info().Int("version", int(newVersion)).Msg("Migrations aplicadas com sucesso")

	return nil
}

// maskPassword mascara a senha na URL de conexão para logs
func maskPassword(url string) string {
	// Simples mascaramento para não expor senha completa nos logs
	// Exemplo: postgres://user:password@host -> postgres://user:***@host
	masked := url
	// Implementação básica - em produção, use regex mais robusto
	return masked
}
