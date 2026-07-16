// NUFS Admin Server - Multi-cluster management backend
package main

import (
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/your-org/nufs-admin/internal/api"
	"github.com/your-org/nufs-admin/internal/auth"
	"github.com/your-org/nufs-admin/internal/cache"
	"github.com/your-org/nufs-admin/internal/cluster"
	"github.com/your-org/nufs-admin/internal/config"
	"github.com/your-org/nufs-admin/internal/proxy"
	"github.com/your-org/nufs-admin/internal/server"
	"github.com/your-org/nufs-admin/internal/store"
)

func main() {
	// Load configuration
	configPath := os.Getenv("NUFS_ADMIN_CONFIG")
	if configPath == "" {
		configPath = "deploy/clusters.yaml"
	}

	// Create Registry placeholder so we can pass it to config onChange callback
	var registry *cluster.Registry

	cfgMgr, err := config.NewManager(configPath, func(cfg *config.Config) {
		// On SIGHUP, reload clusters in registry
		if registry != nil {
			if err := registry.Reload(); err != nil {
				log.Printf("WARN: registry reload on SIGHUP failed: %v", err)
			}
		}
	})
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	cfg := cfgMgr.Get()

	// Load users
	users, err := auth.LoadUsers(cfg.Auth.UsersFile)
	if err != nil {
		log.Fatalf("failed to load users: %v", err)
	}

	// Initialize MySQL store (optional)
	var st *store.Store
	if cfg.Database.DSN != "" {
		db, err := sql.Open("mysql", cfg.Database.DSN)
		if err != nil {
			log.Fatalf("failed to open MySQL: %v", err)
		}
		db.SetMaxOpenConns(20)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(5 * time.Minute)

		if err := db.Ping(); err != nil {
			log.Printf("WARN: MySQL ping failed: %v (continuing without DB)", err)
			// Continue without DB - will use config-only mode
		} else {
			st = store.New(db)
			log.Println("MySQL store initialized")
		}
	} else {
		log.Println("No database DSN configured, using config-only mode")
	}

	// Initialize registry (Hybrid: YAML static + DB dynamic)
	registry, err = cluster.NewRegistry(cfgMgr, st)
	if err != nil {
		log.Fatalf("failed to init registry: %v", err)
	}
	defer registry.Close()

	// Initialize other components
	responseCache := cache.New(10 * time.Second) // 10s TTL for read requests
	requestProxy := proxy.NewProxy(registry, responseCache)
	aggregator := proxy.NewAggregator(requestProxy)
	jwt := auth.NewJWTManager(cfg.Server.JWTSecret)

	// Setup router
	router := api.NewRouter(requestProxy, aggregator, jwt, users, registry)

	// Create and run server
	srv := server.New(cfg.Server.Listen, router)
	if err := srv.Run(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}