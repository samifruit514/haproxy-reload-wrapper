package utils

import (
	"os"
	"io"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

// LookupExecutablePathAbs lookup the $PATH to find the absolut path of an executable
func LookupExecutablePathAbs(executable string) (string, error) {
	file, err := exec.LookPath(executable)
	if err != nil {
		return "", err
	}

	return filepath.Abs(file)
}

// LookupWatchPath return WATCH_PATH if defined
func LookupWatchPath() string {
	return os.Getenv("WATCH_PATH")
}

// LookupHAProxyConfigFile lookup the program arguments to find the config file path (default: "/etc/haproxy/haproxy.cfg")
func LookupHAProxyConfigFile() string {
	file := "/etc/haproxy/haproxy.cfg"
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "-f" && i+1 < len(os.Args) {
			file = os.Args[i+1]
		}
	}

	return file
}

func ReplaceHaproxyConfigFilePath(args []string, newFilePath string) []string {
	newArgs := []string{}
	for i := 1; i < len(args); i++ {
		if args[i] == "-f" && i+1 < len(args) {
			i+=2
		} else {
		}
		newArgs = append(newArgs, args[i])
	}

	return newArgs
}

// LookupHAProxySocketPath lookup the value of HAPROXY_SOCKET environment variable (default:"/var/run/haproxy.sock")
func LookupHAProxySocketPath() string {
	if path, ok := os.LookupEnv("HAPROXY_SOCKET"); ok {
		return path
	}

	return "/var/run/haproxy.sock"
}

// LoadEnvFile load additional dynamic environment variables from a file which contains them in the form "key=value".
func LoadEnvFile() []string {
	env := os.Environ()

	if file, ok := os.LookupEnv("ENV_FILE"); ok {
		if data, err := os.ReadFile(file); err == nil {
			env = append(env, strings.Split(string(data), "/n")...)
		}
	}

	return env
}

func CopyFile(src, dst string) (int64, error) {
        sourceFileStat, err := os.Stat(src)
        if err != nil {
                return 0, err
        }

        if !sourceFileStat.Mode().IsRegular() {
                return 0, fmt.Errorf("%s is not a regular file", src)
        }

        source, err := os.Open(src)
        if err != nil {
                return 0, err
        }
        defer source.Close()

        destination, err := os.Create(dst)
        if err != nil {
                return 0, err
        }
        defer destination.Close()
        nBytes, err := io.Copy(destination, source)
        return nBytes, err
}

func GetEnv(key, fallback string) string {
    if value, ok := os.LookupEnv(key); ok {
        return value
    }
    return fallback
}
