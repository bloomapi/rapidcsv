package main

import (
	"os"
	"fmt"
	"encoding/csv"
	"bitbucket.org/gocodo/bloomsource"
	"bitbucket.org/gocodo/bloomsource/helpers"
	"github.com/gocodo/bloomdb"
	"github.com/spf13/viper"
)

var filePath string
var sourceName string

func showUsage() {
	fmt.Printf("Usage: %s <table> <csv>\n", os.Args[0])
}

type FakeDescription struct {}

func (f *FakeDescription) Available() ([]bloomsource.Source, error) {
	return []bloomsource.Source{
	    bloomsource.Source{
	      Name: sourceName,
	      Version: "20150000",
	    },
  	}, nil
}

func (f *FakeDescription) FieldNames(sourceName string) ([]string, error) {
	fileReader, err := os.Open(filePath)
  if err != nil {
    return nil, err
  }

  csvReader := csv.NewReader(fileReader)
  if err != nil {
    return nil, err
  }

  columns, err := csvReader.Read()
  if err != nil {
    return nil, err
  }

  return columns, nil
}

func (f *FakeDescription) Reader(source bloomsource.Source) (bloomsource.ValueReader, error) {
	fileReader, err := os.Open(filePath)
  if err != nil {
    return nil, err
  }

	csvReader := helpers.NewCsvReader(fileReader)

	return csvReader, nil
}

func main() {
	if (len(os.Args) != 3) {
		fmt.Println("Invalid command usage\n")
		showUsage()
		os.Exit(1)
	}

	sourceName = os.Args[1]
	filePath = os.Args[2]

	// FAKE CONFIG

	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath("./")

	viper.SetDefault("sqlConnStr", "postgres://localhost/workspace?sslmode=disable")

	configPath := os.Getenv("BLOOM_CONFIG")
	if configPath != "" {
		viper.AddConfigPath(configPath)
	}

	err := viper.ReadInConfig()

	desc := &FakeDescription{}

	// GET SCHEMA

	schema, err := bloomsource.Schema(desc)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	mapping := bloomsource.SchemaToMapping(schema)

	// BOOTSTRAP DB

  bdb := bloomdb.DBFromConfig(viper.GetString("sqlConnStr"), viper.GetStringSlice("searchHosts"))

	conn, err := bdb.SqlConnection()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	sql := bloomsource.MappingToTableOnly(mapping)
	indexSql := bloomsource.MappingToIndex(mapping)

	sql = "DROP TABLE IF EXISTS " + sourceName + "; DROP TABLE IF EXISTS " + sourceName + "_revisions; " + sql

	_, err = conn.Exec(sql)
	if err != nil {
		fmt.Println("Error executing", sql)
		fmt.Println(err)
		os.Exit(1)
	}

	_, err = conn.Exec(indexSql)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// INSERT

	sources, err := desc.Available()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	vr, err := desc.Reader(sources[0])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = bloomsource.InsertWithDB(bdb, vr, mapping.Sources[0], []string{sourceName}, "sync")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	_, err = conn.Exec("DROP TABLE IF EXISTS " + sourceName + "_revisions;")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
