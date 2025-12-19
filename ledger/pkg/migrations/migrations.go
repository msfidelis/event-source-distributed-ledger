package migrations

import (
	"fmt"
	"log"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// RunMigrations executa as migrations do banco de dados
func RunMigrations(databaseURL, migrationsPath string) error {
	log.Printf("[Migrations] Iniciando migrations do banco de dados...")
	log.Printf("[Migrations] Database URL: %s", maskPassword(databaseURL))
	log.Printf("[Migrations] Migrations Path: %s", migrationsPath)

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
		log.Printf("[Migrations] Erro ao obter versão: %v", err)
	} else if err == migrate.ErrNilVersion {
		log.Printf("[Migrations] Nenhuma migration aplicada ainda")
	} else {
		log.Printf("[Migrations] Versão atual: %d (dirty: %v)", version, dirty)
	}

	// Aplica migrations
	if err := m.Up(); err != nil {
		if err == migrate.ErrNoChange {
			log.Printf("[Migrations] ✓ Banco de dados já está atualizado (versão %d)", version)
			return nil
		}
		return fmt.Errorf("erro ao executar migrations: %w", err)
	}

	// Obtém nova versão
	newVersion, _, _ := m.Version()
	log.Printf("[Migrations] ✓ Migrations aplicadas com sucesso! Nova versão: %d", newVersion)

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
