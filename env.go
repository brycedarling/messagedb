package messagedb

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

func GetOrDefault(envVar string, defaultValue string) string {
	if val, ok := os.LookupEnv(envVar); ok {
		return val
	}

	return defaultValue
}

func GetBoolOrDefault(envVar string, defaultValue bool) bool {
	val, err := strconv.ParseBool(GetOrDefault(envVar, strconv.FormatBool(defaultValue)))
	if err != nil {
		log.Fatalf("Error parsing boolean from environment variable '%s': %s", envVar, err)
	}
	return val
}

func GetIntOrDefault(envVar string, defaultValue int) int64 {
	val, err := strconv.ParseInt(GetOrDefault(envVar, strconv.Itoa(defaultValue)), 10, 64)
	if err != nil {
		log.Fatalf("Error parsing integer value from environment variable '%s': %s", envVar, err)
	}
	return val
}

func GetOrPanic(envVar string) string {
	if val, ok := os.LookupEnv(envVar); ok {
		return val
	}

	panic(fmt.Sprintf("Missing value for environment variable '%s'", envVar))
}

func GetOrErr(envVar string) (string, error) {
	if val, ok := os.LookupEnv(envVar); ok {
		return val, nil
	}

	return "", fmt.Errorf("Missing value for environment variable '%s'", envVar)
}
