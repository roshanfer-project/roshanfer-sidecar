package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var cancel context.CancelFunc
var ctx context.Context
var wg *sync.WaitGroup

func main() {
	wg = &sync.WaitGroup{}

	serviceList := [][]string{
		{"app", "9", "0"},
	}

	serviceList = append(serviceList, []string{"ingress", "_", "_"})

	// listen for SIGINT (Ctrl-C)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		fmt.Println("Received Ctrl-C, cancelling context")
		cancel()
		wg.Wait()
		os.Exit(0)
	}()

	ctx, cancel = context.WithCancel(context.Background())

	// run memcached and mongodb
	//run_docker_compose()

	time.Sleep(time.Second * 2)

	// compile sidecar
	compile_sidecar(false)

	env := ".env"

	run_servicees(env, serviceList, true, false, false)

	time.Sleep(time.Minute * 100)

	cancel()
	wg.Wait()

}

func run_docker_compose() {
	folder := "."
	dir := get_cwd() + "/" + folder
	c := exec.CommandContext(ctx, "docker", "compose", "-f", "docker-compose.yaml", "up", "-d")
	no_env_run(c, dir, false, "docker-compose")
}

func run_servicees(env string, serviceList [][]string, sidecar, envoy, profile bool) {
	sidecar_dir := get_cwd() + "/service-mesh"
	for _, tuple := range serviceList {
		name := tuple[0]
		cpuset := tuple[1]
		//sidecar_cpuset := tuple[2]

		fmt.Println("/////////////////////////", name, "/////////////////////////")
		if name != "ingress" {
			// build the service
			folder := fmt.Sprintf("../%s", name)
			dir := get_cwd() + "/" + folder
			c := exec.CommandContext(ctx, "go", "build", "-o", fmt.Sprintf("%s.o", name), ".")
			c.Dir = dir
			no_env_run(c, dir, false, name)

			// run the service
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				fmt.Printf("Running %s\n", name)
				c = exec.CommandContext(ctx, "taskset", "-c", cpuset, fmt.Sprintf("./%s.o", name))
				//c = exec.CommandContext(ctx, fmt.Sprintf("./%s.o", name))
				env_run(c, dir, env)
			}(name)
		}

		// run the sidecar
		if sidecar || envoy {
			if sidecar {
				c := exec.CommandContext(ctx, "docker", "compose", "run", "-d", "-T", "-P",
					"--name", fmt.Sprintf("%s-sidecar", name), fmt.Sprintf("%s-sidecar", name))
				c.Dir = sidecar_dir
				no_env_run(c, sidecar_dir, false, "docker-compose")

				if profile && name == "search" {
					c_prof := exec.Command("/bin/bash", get_cwd()+"/profile.sh", fmt.Sprintf("%s-sidecar", name))
					c_prof.Stdout = os.Stdout
					c_prof.Stderr = os.Stderr
					c_prof.Start()
				}
			} else {
				c := exec.CommandContext(ctx, "docker", "compose", "-f", "envoy-compose.yaml", "run", "-d", "-T", "-P",
					"--name", fmt.Sprintf("%s-envoy", name), fmt.Sprintf("%s-envoy", name))
				c.Dir = sidecar_dir
				no_env_run(c, sidecar_dir, false, "envoy-compose")
			}
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func compile_sidecar(profile bool) {
	dir := get_cwd() + "/../.."
	fmt.Printf("Compiling sidecar in %s\n", dir)
	var build_type string
	if profile {
		build_type = "Debug"
	} else {
		build_type = "Release"
	}

	c := exec.Command("/bin/bash", dir+"/build.sh", build_type)
	no_env_run(c, dir, false, "sidecar")

	c = exec.Command("docker", "compose", "build")
	no_env_run(c, dir, false, "docker-compose")
}

func read_env(name string) []string {
	envFile, err := os.ReadFile(name)
	if err != nil {
		fmt.Println("Error reading .env file:", err)
		cancel()
		panic(err)
	}
	envs := make([]string, 0)
	lines := strings.Split(string(envFile), "\n")
	for _, line := range lines {
		if line == "" || line[0] == '#' {
			continue // skip empty lines and comments
		}
		if !strings.Contains(line, "=") {
			continue // skip lines without '='
		}
		envs = append(envs, line)
	}

	// log the environment variables
	/* for _, env := range envs {
		fmt.Println("Environment variable:", env)
	} */
	return envs
}

func no_env_run(c *exec.Cmd, dir string, profile bool, name string) {
	c.Dir = dir
	/* f, err := os.OpenFile(outputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Printf("Error opening file %s: %v\n", outputFile, err)
		cancel()
		panic(err)
	}
	defer f.Close()

	multiWriter := io.MultiWriter(os.Stdout, f)
	c.Stdout = multiWriter
	c.Stderr = multiWriter */
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	// create a new process group for the command
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// start the command
	if err := c.Start(); err != nil {
		cancel()
		panic(err)
	}

	pid := c.Process.Pid

	var prof_c *exec.Cmd
	if profile {
		prof_c = exec.Command("perf", "record", "-F", "999", "-g", "-p", fmt.Sprintf("%d", pid),
			"-o", fmt.Sprintf("%s.prof", name), "--call-graph", "dwarf")
		prof_c.Stdout = os.Stdout
		prof_c.Stderr = os.Stderr
		//prof_c.Dir = dir
		prof_c.Start()
	}

	done := make(chan error)
	go func() {
		done <- c.Wait()
	}()

	select {
	case err := <-done:
		if err != nil {
			cancel()
			panic(err)
		}
	case <-ctx.Done():
		// on cancellation, kill the entire process group
		// negative PID kills the process group
		fmt.Println("Killing process group")
		syscall.Kill(-pid, syscall.SIGKILL)
		if prof_c != nil {
			prof_c.Wait()
		}
		<-done // wait for c.Wait() to return
	}
}

func env_run(c *exec.Cmd, dir, env string) {
	c.Dir = dir
	/* f, err := os.OpenFile(outputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Printf("Error opening file %s: %v\n", outputFile, err)
		cancel()
		panic(err)
	}
	defer f.Close()

	multiWriter := io.MultiWriter(os.Stdout, f)
	c.Stdout = multiWriter
	c.Stderr = multiWriter */
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	c.Env = read_env(env)
	// create a new process group for the command
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := c.Start(); err != nil {
		cancel()
		panic(err)
	}

	pid := c.Process.Pid

	done := make(chan error)
	go func() {
		done <- c.Wait()
	}()

	select {
	case err := <-done:
		if err != nil {
			cancel()
			panic(err)
		}
	case <-ctx.Done():
		// on cancellation, kill the entire process group
		fmt.Println("Killing process group")
		syscall.Kill(-pid, syscall.SIGKILL)
		<-done
	}
}

func get_cwd() string {
	cwd, err := os.Getwd()
	if err != nil {
		cancel()
		panic(err)
	}
	return cwd
}
