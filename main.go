package main

import (
	"context"
	"fmt"
    "io"
	"os"
	"os/signal"
	"path/filepath"
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

var k8s_watch_path = "/haproxy_k8s.cfg"

func copy(src, dst string) (int64, error) {
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

func startCMWatcher(k8sCMName string) {

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
	writePath := k8s_watch_path
	sourcePathForCopy := utils.LookupHAProxyConfigFile()
	nBytes, err := copy(sourcePathForCopy, writePath)
	if err != nil {
		panic(fmt.Sprintf("Unable to copy %s to %s: %s", sourcePathForCopy, writePath, err))
	}
	log.Notice(fmt.Sprintf("File copied %s to %s: %d bytes written", sourcePathForCopy, writePath, nBytes))
	
	log.Notice("start wait for changes")
	go watchForChanges(clientset, k8sCMName, namespace, writePath)

}

func watchForChanges(clientset *kubernetes.Clientset, cm_name, namespace, writePath string) {
	for {
		watcher, err := clientset.CoreV1().ConfigMaps(namespace).Watch(context.TODO(),
			metav1.SingleObject(metav1.ObjectMeta{Name: cm_name, Namespace: namespace}))
		if err != nil {
			panic("Unable to create watcher")
		}
		updateCMFile(watcher.ResultChan(), writePath)
	}
}

func updateCMFile(eventChannel <-chan watch.Event, writePath string) {
	file_in_cm := filepath.Base(writePath)
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
						log.Notice(fmt.Sprintf("Is %s == %s?", k, file_in_cm))
						if k == file_in_cm {
							log.Notice("writing to: " + writePath)
							err := os.WriteFile(writePath, []byte(v), 0644)
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

func main() {
	// fetch the absolut path of the haproxy executable
	executable, err := utils.LookupExecutablePathAbs("haproxy")
	if err != nil {
		log.Emergency(err.Error())
		os.Exit(1)
	}
	k8sCMName := os.Getenv("K8S_CM_NAME")
	k8sWatcherEnabled := (k8sCMName != "")
	if k8sWatcherEnabled {
		log.Notice(fmt.Sprintf("Starting watcher for configmap: %s", k8sCMName))
		startCMWatcher(k8sCMName)
	}

	// execute haproxy with the flags provided as a child process asynchronously
	cmd := exec.Command(executable, os.Args[1:]...)
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
	if err := fswatch.Add(watchPath); err != nil {
		log.Notice(fmt.Sprintf("watch failed : %v", err))
		os.Exit(1)
	}
	log.Notice(fmt.Sprintf("watch : %s", watchPath))

	// flag used for termination handling
	var terminated bool

	// initialize a signal handler for SIGINT, SIGTERM and SIGUSR1 (for OpenShift)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)

	// endless for loop which handles signals, file system events as well as termination of the child process
	for {
		select {
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

			// create a new haproxy process which will replace the old one after it was successfully started
			tmp := exec.Command(executable, append([]string{"-x", utils.LookupHAProxySocketPath(), "-sf", strconv.Itoa(cmd.Process.Pid)}, os.Args[1:]...)...)
			tmp.Stdout = os.Stdout
			tmp.Stderr = os.Stderr
			tmp.Env = utils.LoadEnvFile()

			if err := tmp.AsyncRun(); err != nil {
				log.Warning(err.Error())
				log.Warning("reload failed")
				continue
			}

			log.Notice(fmt.Sprintf("process %d started", tmp.Process.Pid))
			select {
			case <-cmd.Terminated:
				// old haproxy terminated - successfully started a new process replacing the old one
				log.Notice(fmt.Sprintf("process %d terminated : %s", cmd.Process.Pid, cmd.Status()))
				log.Notice("reload successful")
				cmd = tmp
			case <-tmp.Terminated:
				// new haproxy terminated without terminating the old process - this can happen if the modified configuration file was invalid
				log.Warning(fmt.Sprintf("process %d terminated unexpectedly : %s", tmp.Process.Pid, tmp.Status()))
				log.Warning("reload failed")
			}
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
