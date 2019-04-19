package cmd

import (
	"log"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "fdb-gateway",
	Short: "fdb-gateway",
	Long:  `fdb-gateway`,
}

var cmdServer = &cobra.Command{
	Use:   "server",
	Short: "Start fdb-gateway server",
	Long:  `Start fdb-gateway server`,
	Run:   Server,
}

//var version = "dev" // set by release script

func Execute() {
	ServerBindFlags()

	rootCmd.AddCommand(cmdServer)

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
