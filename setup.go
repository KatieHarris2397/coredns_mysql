package coredns_mysql

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
)

const (
	defaultTtl                = 360
	defaultMaxLifeTime        = 1 * time.Minute
	defaultMaxOpenConnections = 10
	defaultMaxIdleConnections = 10
	defaultZoneUpdateTime     = 10 * time.Minute
)

func init() {
	fmt.Println("[INIT] Registering MySQL plugin")
	caddy.RegisterPlugin("mysql", caddy.Plugin{
		ServerType: "dns",
		Action:     setup,
	})
}

func setup(c *caddy.Controller) error {
	fmt.Println("[SETUP] Setting up MySQL plugin")
	r, err := mysqlParse(c)
	if err != nil {
		fmt.Printf("[ERROR] Failed to parse MySQL configuration: %v\n", err)
		return plugin.Error("mysql", err)
	}

	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		fmt.Println("[SETUP] Adding MySQL plugin to CoreDNS")
		r.Next = next
		return r
	})

	return nil
}

func mysqlParse(c *caddy.Controller) (*CoreDNSMySql, error) {
	fmt.Println("[PARSE] Parsing MySQL configuration")
	mysql := CoreDNSMySql{
		TablePrefix: "coredns_",
		Ttl:         300,
	}
	var err error

	c.Next()
	if c.NextBlock() {
		for {
			switch c.Val() {
			case "dsn":
				if !c.NextArg() {
					return &CoreDNSMySql{}, c.ArgErr()
				}
				mysql.Dsn = c.Val()
				fmt.Printf("[CONFIG] Setting DSN: %s\n", mysql.Dsn)
			case "table_prefix":
				if !c.NextArg() {
					return &CoreDNSMySql{}, c.ArgErr()
				}
				mysql.TablePrefix = c.Val()
				fmt.Printf("[CONFIG] Setting table prefix: %s\n", mysql.TablePrefix)
			case "max_lifetime":
				if !c.NextArg() {
					return &CoreDNSMySql{}, c.ArgErr()
				}
				var val time.Duration
				val, err = time.ParseDuration(c.Val())
				if err != nil {
					val = defaultMaxLifeTime
				}
				mysql.MaxLifetime = val
			case "max_open_connections":
				if !c.NextArg() {
					return &CoreDNSMySql{}, c.ArgErr()
				}
				var val int
				val, err = strconv.Atoi(c.Val())
				if err != nil {
					val = defaultMaxOpenConnections
				}
				mysql.MaxOpenConnections = val
			case "max_idle_connections":
				if !c.NextArg() {
					return &CoreDNSMySql{}, c.ArgErr()
				}
				var val int
				val, err = strconv.Atoi(c.Val())
				if err != nil {
					val = defaultMaxIdleConnections
				}
				mysql.MaxIdleConnections = val
			case "zone_update_interval":
				if !c.NextArg() {
					return &CoreDNSMySql{}, c.ArgErr()
				}
				var val time.Duration
				val, err = time.ParseDuration(c.Val())
				if err != nil {
					val = defaultZoneUpdateTime
				}
				mysql.zoneUpdateTime = val
			case "ttl":
				if !c.NextArg() {
					return &CoreDNSMySql{}, c.ArgErr()
				}
				var val int
				val, err = strconv.Atoi(c.Val())
				if err != nil {
					val = defaultTtl
				}
				mysql.Ttl = uint32(val)
			default:
				if c.Val() != "}" {
					return &CoreDNSMySql{}, c.Errf("unknown property '%s'", c.Val())
				}
			}

			if !c.Next() {
				break
			}
		}

	}

	fmt.Println("[PARSE] Testing database connection")
	db, err := mysql.db()
	if err != nil {
		fmt.Printf("[ERROR] Database connection failed: %v\n", err)
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		fmt.Printf("[ERROR] Database ping failed: %v\n", err)
		return nil, err
	}
	defer db.Close()

	mysql.tableName = mysql.TablePrefix + "records"
	fmt.Printf("[PARSE] MySQL plugin configured successfully. Table name: %s\n", mysql.tableName)

	return &mysql, nil
}

func (handler *CoreDNSMySql) db() (*sql.DB, error) {
	fmt.Printf("[DB] Attempting to connect to MySQL with DSN: %s\n", os.ExpandEnv(handler.Dsn))
	db, err := sql.Open("mysql", os.ExpandEnv(handler.Dsn))
	if err != nil {
		fmt.Printf("[ERROR] Error opening database connection: %v\n", err)
		return nil, err
	}

	db.SetConnMaxLifetime(handler.MaxLifetime)
	db.SetMaxOpenConns(handler.MaxOpenConnections)
	db.SetMaxIdleConns(handler.MaxIdleConnections)

	// Test the connection
	err = db.Ping()
	if err != nil {
		fmt.Printf("[ERROR] Error pinging database: %v\n", err)
		return nil, err
	}
	fmt.Println("[DB] Successfully connected to MySQL database")

	return db, nil
}
