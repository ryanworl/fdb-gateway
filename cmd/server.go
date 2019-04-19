package cmd

import (
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"

	"github.com/ryanworl/fdb-gateway/pkg/server"
	"github.com/ryanworl/fdb-gateway/pkg/tenancy"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var addr string
var apiVersion int
var clusterFile string
var debugLogging bool
var layerName string

func ServerBindFlags() {
	cmdServer.PersistentFlags().IntVar(&apiVersion, "fdb-api-version", 600, "FoundationDB API Version")
	err := viper.BindPFlag("fdb-api-version", cmdServer.PersistentFlags().Lookup("fdb-api-version"))
	if err != nil {
		log.Fatal(err)
	}

	cmdServer.PersistentFlags().StringVar(&addr, "bind", ":6380", "Address to bind to")
	err = viper.BindPFlag("bind", cmdServer.PersistentFlags().Lookup("bind"))
	if err != nil {
		log.Fatal(err)
	}

	cmdServer.PersistentFlags().StringVar(&clusterFile, "cluster-file", "", "FoundationDB Cluster File")
	err = viper.BindPFlag("cluster-file", cmdServer.PersistentFlags().Lookup("cluster-file"))
	if err != nil {
		log.Fatal(err)
	}

	cmdServer.PersistentFlags().BoolVar(&debugLogging, "debug-log-commands", false, "Log FDB operations (get_range, set, clear, etc.)")
	err = viper.BindPFlag("debug-log-commands", cmdServer.PersistentFlags().Lookup("debug-log-commands"))
	if err != nil {
		log.Fatal(err)
	}

	cmdServer.PersistentFlags().StringVar(&layerName, "layer-name", "gateway", "Name to use in the FoundationDB Directory Layer")
	err = viper.BindPFlag("layer-name", cmdServer.PersistentFlags().Lookup("layer-name"))
	if err != nil {
		log.Fatal(err)
	}
}

func OpenDB() fdb.Database {
	if clusterFile == "" {
		return fdb.MustOpenDefault()
	}

	return fdb.MustOpen(clusterFile, []byte("DB"))
}

func Server(cmd *cobra.Command, args []string) {
	fdb.MustAPIVersion(apiVersion)

	db := OpenDB()

	rootDirectory := directory.Root()
	tenantAuthorizationDirectory, err := rootDirectory.CreateOrOpen(db, []string{"sys-tenant-authorization"}, []byte(layerName))
	if err != nil {
		log.Fatal(err)
	}

	tenantAuthorizer := tenancy.NewTenantAuthorizer(db, tenantAuthorizationDirectory)

	log.Printf("started %s at %s", cmd.Root().Name(), addr)

	s := server.NewServer(db, tenantAuthorizer, rootDirectory)

	err = s.Serve(addr)
	if err != nil {
		log.Fatal(err)
	}
}
