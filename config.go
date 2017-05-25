package setcd

type config struct {
	Delimiters []string
	MD         mdConfig
}

type mdConfig struct {
	RootDir      string
	LenSubDir    string
	KindSubDir   string
	TagsSubDir   string
	IdxesSubDir  string
	LastIDSubDir string
}

var Config config

func init() {
	Config = config{
		Delimiters: []string{"{{", "}}"},
		MD: mdConfig{
			RootDir:      "/__metadata__",
			LenSubDir:    "__len__",
			KindSubDir:   "__kind__",
			TagsSubDir:   "__tags__",
			IdxesSubDir:  "__idxes__",
			LastIDSubDir: "__lastID__",
		},
	}
}
