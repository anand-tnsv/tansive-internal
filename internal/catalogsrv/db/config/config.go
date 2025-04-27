package config

import (
	"fmt"

	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
)

type dbconncfg struct {
	host     string
	port     int
	dbname   string
	user     string
	password string
	sslmode  string
}

var hatchCatalogDbConn *dbconncfg

func init() {
	hatchCatalogDbConn = &dbconncfg{
		host:     "localhost",
		port:     5432,
		user:     "catalog_api",
		password: "abc@123",
		dbname:   "hatchcatalog",
		sslmode:  "disable",
	}
}

func HatchCatalogDsn() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		hatchCatalogDbConn.host, hatchCatalogDbConn.port, hatchCatalogDbConn.user, hatchCatalogDbConn.password, hatchCatalogDbConn.dbname, hatchCatalogDbConn.sslmode)
}

const CompressCatalogObjects = config.CompressCatalogObjects
