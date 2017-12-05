package pbft

import (
	fmt "fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

const configPrefix = "CORE_PBFT"

func loadConfig() (config *viper.Viper) {
	config = viper.New()

	config.SetEnvPrefix(configPrefix)
	config.AutomaticEnv()
	replacer := strings.NewReplacer(".", "_")
	config.SetEnvKeyReplacer(replacer)

	config.SetConfigName("config")
	config.AddConfigPath("./")
	config.AddConfigPath("../")

	gopath := os.Getenv("GOPATH")
	for _, p := range filepath.SplitList(gopath) {
		pbftpath := filepath.Join(p, "src/github.com/kentwangyq/pbft/pbft")
		config.AddConfigPath(pbftpath)
	}

	err := config.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Error reading %s plugin config: %s", configPrefix, err))
	}
	return
}
