package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	//"path/filepath"
	"time"
	"strconv"
	"syscall"

	"github.com/fsnotify/fsnotify"
	"github.com/snorwin/haproxy-reload-wrapper/pkg/exec"
	"github.com/snorwin/haproxy-reload-wrapper/pkg/log"
	"github.com/snorwin/haproxy-reload-wrapper/pkg/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func startCMWatcher(k8sCMName, k8sCMKey, k8sCMWatchPath string, chConfig chan bool) {

	ns_filename := "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	nsBytes, err := os.ReadFile(ns_filename)
	if err != nil {
		panic(fmt.Sprintf("Unable to read namespace file at %s", ns_filename))
	}

	namespace := string(nsBytes)

	clientCfg, err := rest.InClusterConfig()
	if err != nil {
		panic("Unable to get our client configuration")
	}

	clientset, err := kubernetes.NewForConfig(clientCfg)
	if err != nil {
		panic("Unable to create our clientset")
	}
	// k8s watches for configmap updates and we are about to watch
    // the same file through the api. lets avoid conflicts by
    // creating a local copy from the mounted cm
	writePath := k8sCMWatchPath
	sourcePathForCopy := utils.LookupHAProxyConfigFile()
	nBytes, err := utils.CopyFile(sourcePathForCopy, writePath)
	if err != nil {
		panic(fmt.Sprintf("Unable to copy %s to %s: %s", sourcePathForCopy, writePath, err))
	}
	log.Notice(fmt.Sprintf("File copied %s to %s: %d bytes written", sourcePathForCopy, writePath, nBytes))
	
	log.Notice("start wait for changes")
	go watchForChanges(clientset, k8sCMName, k8sCMKey, namespace, writePath, chConfig)

}

func watchForChanges(clientset *kubernetes.Clientset, cm_name, cm_key, namespace, writePath string, chConfig chan bool) {
	for {
		watcher, err := clientset.CoreV1().ConfigMaps(namespace).Watch(context.TODO(),
			metav1.SingleObject(metav1.ObjectMeta{Name: cm_name, Namespace: namespace}))
		if err != nil {
			panic("Unable to create watcher")
		}
		updateCMFile(watcher.ResultChan(), writePath, cm_key, chConfig)
	}
}
// gracefulShutdowner gives 5 seconds to the new process to handle connections
// exclude old process from accepting connections (but continue requests)
// and wait graceful_timeout seconds so exchanges eventually disconnect
func gracefulShutdowner(chPids chan int, graceful_timeout int) {
	for {
		select {
		case pid := <-chPids:
			fmt.Println(pid)

			go func() {
				select {
				case <-time.After(time.Second * 5):
					log.Notice(fmt.Sprintf("stop listening for pid %d (SIGTTOU)", pid))
					syscall.Kill(pid,syscall.SIGTTOU)

				}
				select {
				case <-time.After(time.Second * time.Duration(graceful_timeout)):
					log.Notice(fmt.Sprintf("kill pid %d (SIGTERM)", pid))
					syscall.Kill(pid,syscall.SIGTERM)

				}
			}()

		}
	}
}

func updateCMFile(eventChannel <-chan watch.Event, writePath, cm_key string, chConfig chan bool) {
	//key_in_cm := filepath.Base(writePath)
	for {
		event, open := <-eventChannel
		if open {
			switch event.Type {
			case watch.Added:
				fallthrough
			case watch.Modified:
				// Update our endpoint
				if updatedMap, ok := event.Object.(*corev1.ConfigMap); ok {
					for k, v := range updatedMap.Data {
						log.Notice(fmt.Sprintf("Is %s == %s?", k, cm_key))
						if k == cm_key {
							log.Notice("writing to: " + writePath)
							err := os.WriteFile(writePath, []byte(v), 0644)
							chConfig <- true
							if err != nil {
								log.Emergency(err.Error())
							}

						}
					}

				}
			default:
				// Do nothing
			}
		} else {
			// If eventChannel is closed, it means the server has closed the connection
			return
		}
	}
}

func validateConfig(executable string, cmdArgs []string) bool {

	tmp := exec.Command(executable, append(cmdArgs,"-c")...)
	tmp.Stdout = os.Stdout
	tmp.Stderr = os.Stderr
	tmp.Env = utils.LoadEnvFile()

	if err := tmp.AsyncRun(); err != nil {
		log.Warning(err.Error())
		log.Warning("reload failed")
		return false
	}
	select {
	case <-tmp.Terminated:
		return tmp.ProcessState.ExitCode() == 0
	}
	return false
}

func main() {
	// fetch the absolut path of the haproxy executable
	executable, err := utils.LookupExecutablePathAbs("haproxy")
	if err != nil {
		log.Emergency(err.Error())
		os.Exit(1)
	}
	k8sCMName := os.Getenv("K8S_CM_NAME")
	k8sCMKey := utils.GetEnv("K8S_CM_KEY", "haproxy.cfg")
	k8sWatcherEnabled := (k8sCMName != "")
	k8s_watch_path := utils.GetEnv("K8S_WATCH_PATH", "/haproxy_k8s.cfg")
	graceful_timeout := utils.GetEnv("HAPROXY_GRACEFUL_TIMEOUT_SEC", "300")
	graceful_timeout_int, err := strconv.Atoi(graceful_timeout)
    if err != nil {
		panic(err)
	}
	// copy the os.Args so we dont affect the actual os.Args
	finalArgs := make([]string, len(os.Args))
	copy(finalArgs, os.Args)
	finalArgs = utils.ReplaceHaproxyConfigFilePath(finalArgs, k8s_watch_path)
	
	// direct chan, instead of writing to fs
	chConfig := make(chan bool)
	if k8sWatcherEnabled {
		log.Notice(fmt.Sprintf("Starting watcher for configmap: %s", k8sCMName))
		startCMWatcher(k8sCMName, k8sCMKey, k8s_watch_path, chConfig)

	}

	// execute haproxy with the flags provided as a child process asynchronously
	fmt.Println(finalArgs[1:])
	cmd := exec.Command(executable, finalArgs[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = utils.LoadEnvFile()
	if err := cmd.AsyncRun(); err != nil {
		log.Emergency(err.Error())
		os.Exit(1)
	}
	log.Notice(fmt.Sprintf("process %d started", cmd.Process.Pid))

	watchPath := utils.LookupWatchPath()
	if k8sWatcherEnabled {
		watchPath = k8s_watch_path
	} else {
		if watchPath == "" {
			watchPath = utils.LookupHAProxyConfigFile()
		}
	}

	// create a fsnotify.Watcher for config changes
	fswatch, err := fsnotify.NewWatcher()
	if err != nil {
		log.Notice(fmt.Sprintf("fsnotify watcher create failed : %v", err))
		os.Exit(1)
	}
	if !k8sWatcherEnabled {
		if err := fswatch.Add(watchPath); err != nil {
			log.Notice(fmt.Sprintf("watch failed : %v", err))
			os.Exit(1)
		}
	}
	log.Notice(fmt.Sprintf("watch : %s", watchPath))

	// flag used for termination handling
	var terminated bool

	// initialize a signal handler for SIGINT, SIGTERM and SIGUSR1 (for OpenShift)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)

	pidsChan := make(chan int, 1)
	go gracefulShutdowner(pidsChan, graceful_timeout_int)

	// endless for loop which handles signals, file system events as well as termination of the child process
	//pids := map[int]struct{}{}
	for {
		select {
		case <-chConfig:
			cmdArgs := finalArgs[1:]
			configIsValid := validateConfig(executable, cmdArgs)
			if configIsValid {
				log.Notice("config is valid")
			} else {
				log.Warning("config is INVALID. no reload!")
				continue
			}
			tmp := exec.Command(executable, cmdArgs...)
			tmp.Stdout = os.Stdout
			tmp.Stderr = os.Stderr
			tmp.Env = utils.LoadEnvFile()

			if err := tmp.AsyncRun(); err != nil {
				log.Warning(err.Error())
				log.Warning("reload failed")
				continue
			}

			pidsChan <- cmd.Process.Pid
			log.Notice(fmt.Sprintf("process %d started", tmp.Process.Pid))
			cmd = tmp
		case event := <-fswatch.Events:
			// only care about events which may modify the contents of the directory
			if !(event.Has(fsnotify.Write) || event.Has(fsnotify.Remove) || event.Has(fsnotify.Create)) {
				continue
			}

			log.Notice(fmt.Sprintf("fs event for %s : %v", watchPath, event.Op))

			// re-add watch if path was removed - config maps are updated by removing/adding a symlink
			if event.Has(fsnotify.Remove) {
				if err := fswatch.Add(watchPath); err != nil {
					log.Alert(fmt.Sprintf("watch failed : %v", err))
				} else {
					log.Notice(fmt.Sprintf("watch : %s", watchPath))
				}
			}

			// create a new haproxy process which will start serving at the same time
			// of the others
			cmdArgs := finalArgs[1:]
			configIsValid := validateConfig(executable, cmdArgs)
			if configIsValid {
				log.Notice("config is valid")
			} else {
				log.Warning("config is INVALID. no reload!")
				continue
			}


			tmp := exec.Command(executable, cmdArgs...)
			tmp.Stdout = os.Stdout
			tmp.Stderr = os.Stderr
			tmp.Env = utils.LoadEnvFile()

			if err := tmp.AsyncRun(); err != nil {
				log.Warning(err.Error())
				log.Warning("reload failed")
				continue
			}

			log.Notice(fmt.Sprintf("process %d started", tmp.Process.Pid))
			// send old pid in the finish channel
			pidsChan <- cmd.Process.Pid
			cmd = tmp
		case err := <-fswatch.Errors:
			// handle errors of fsnotify.Watcher
			log.Alert(err.Error())
		case sig := <-sigs:
			// handle SIGINT, SIGTERM, SIGUSR1 and propagate it to child process
			log.Notice(fmt.Sprintf("received singal %d", sig))

			if cmd.Process == nil {
				// received termination suddenly before child process was even started
				os.Exit(0)
			}

			// set termination flag before propagating the signal in order to prevent race conditions
			terminated = true

			// propagate signal to child process
			if err := cmd.Process.Signal(sig); err != nil {
				log.Warning(fmt.Sprintf("propagating signal %d to process %d failed", sig, cmd.Process.Pid))
			}
		case <-cmd.Terminated:
			// check for unexpected termination
			if !terminated {
				log.Emergency(fmt.Sprintf("process %d teminated unexpectedly : %s", cmd.Process.Pid, cmd.Status()))
				if cmd.ProcessState != nil && cmd.ProcessState.ExitCode() != 0 {
					os.Exit(cmd.ProcessState.ExitCode())
				} else {
					os.Exit(1)
				}
			}

			log.Notice(fmt.Sprintf("process %d terminated : %s", cmd.Process.Pid, cmd.Status()))
			os.Exit(cmd.ProcessState.ExitCode())
		}
	}
}
