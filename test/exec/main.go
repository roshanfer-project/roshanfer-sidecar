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

	"github.com/alexflint/go-arg"
)

var cancel context.CancelFunc
var ctx context.Context
var wg *sync.WaitGroup

type Args struct {
	Test      string   `arg:"required" help:"Test to run: test1, test2"`
	Plain     bool     `arg:"-p" help:"Run in plain mode"`
	Sidecar   bool     `arg:"-s" help:"Run in sidecar mode"`
	Envoy     bool     `arg:"-e" help:"Run in envoy mode"`
	Overrides []string `arg:"--override,-o" help:"Override env vars (KEY=VALUE)"`
}

func main() {
	var args Args
	arg.MustParse(&args)

	if args.Sidecar {
		args.Plain = false
	}
	if args.Envoy {
		args.Plain = false
		args.Sidecar = false
	}

	if !args.Sidecar && !args.Plain && !args.Envoy {
		fmt.Println("Either -p, -s or -e must be specified")
		os.Exit(1)
	}

	// remove /tmp/HOTEL.ready if it exists
	if _, err := os.Stat("/tmp/TEST.ready"); err == nil {
		os.Remove("/tmp/TEST.ready")
	}

	wg = &sync.WaitGroup{}

	serviceList := make([][]string, 0)

	switch args.Test {
	case "test1":
		serviceList = [][]string{
			{"app", "6,7,8,9,10", "0"},
		}
	case "test2":
		serviceList = [][]string{
			{"backend1", "6,7,8,9,10", "0"},
			{"app", "14,15,16,17,18,19,20", "0"},
		}
	case "test3":
		serviceList = [][]string{
			{"backend1", "9,11", "0"},
			{"backend2", "13,15,17,19", "0"},
			{"app", "21,7", "0"},
		}
	case "b1":
		serviceList = [][]string{
			{"app-busy", "9,11", "0"},
		}
	case "b2":
		serviceList = [][]string{
			{"app-busy-chain", "9,11", "0"},
			{"backend1-b", "13,15", "0"},
		}
	case "plain-chain3":
		serviceList = [][]string{
			{"chain-node-1", "6,7", "0"},
			{"chain-node-2", "11,12", "0"},
			{"chain-node-3", "16,17,18,19,20", "0"},
		}
	}

	/* serviceList := [][]string{
		{"app", "9", "0"},
	} */
	if args.Sidecar || args.Envoy {
		serviceList = append(serviceList, []string{"ingress", "_", "_"})
	}

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

	var env string
	switch args.Test {
	case "test1":
		if args.Plain {
			env = "service-mesh-test1/plain.env"
		} else {
			env = "service-mesh-test1/sidecar.env"
		}
	case "test2":
		if args.Plain {
			env = "service-mesh-test2/plain.env"
		} else {
			env = "service-mesh-test2/sidecar.env"
		}
	case "test3":
		if args.Plain {
			env = "service-mesh-test3/plain.env"
		} else {
			env = "service-mesh-test3/sidecar.env"
		}
	case "b1":
		if args.Plain {
			env = "service-mesh-b1/plain.env"
		} else {
			env = "service-mesh-b1/sidecar.env"
		}
	case "b2":
		if args.Plain {
			env = "service-mesh-b2/plain.env"
		} else {
			env = "service-mesh-b2/sidecar.env"
		}
	case "plain-chain3":
		if args.Plain {
			env = "service-mesh-plain-chain3/plain.env"
		} else {
			fmt.Println("plain-chain3 only supports plain mode")
			os.Exit(1)
		}
	}

	run_servicees(env, args.Test, serviceList, args.Sidecar, args.Envoy, args.Plain, false, args.Overrides)

	// create /tmp/HOTEL.ready
	_, err := os.Create("/tmp/TEST.ready")
	if err != nil {
		fmt.Println("Error creating /tmp/TEST.ready:", err)
		cancel()
		panic(err)
	}

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

func run_servicees(env string, testName string, serviceList [][]string, sidecar, envoy, plain, profile bool, overrides []string) {
	sidecar_dir := get_cwd() + "/service-mesh-" + testName
	for _, tuple := range serviceList {
		name := tuple[0]
		cpuset := tuple[1]
		//sidecar_cpuset := tuple[2]

		fmt.Println("/////////////////////////", name, "/////////////////////////")
		if name != "ingress" {
			// build the service
			var folder string
			if strings.HasPrefix(name, "chain-node-") {
				folder = "../app-chain"
			} else {
				folder = fmt.Sprintf("../%s", name)
			}
			dir := get_cwd() + "/" + folder
			if name == "app-cpp" {
				name = "app"
				c := exec.CommandContext(ctx, "make")
				c.Dir = dir
				no_env_run(c, dir, false, name+"-build")
			} else {
				c := exec.CommandContext(ctx, "go", "build", "-o", fmt.Sprintf("%s.o", name), ".")
				c.Dir = dir
				no_env_run(c, dir, false, name)
			}

			// run the service
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				fmt.Printf("Running %s\n", name)
				c := exec.CommandContext(ctx, "taskset", "-c", cpuset, fmt.Sprintf("./%s.o", name))
				//c = exec.CommandContext(ctx, fmt.Sprintf("./%s.o", name))
				// Inject SERVICE_NAME for chain nodes (or all nodes if useful)
				overrides = append(overrides, fmt.Sprintf("SERVICE_NAME=%s", name))
				env_run(c, dir, env, overrides)
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

		// if plain, run ingress-envoy
		if plain {
			c := exec.CommandContext(ctx, "docker", "compose", "run", "-d", "-T", "-P",
				"--name", "ingress-envoy", "ingress-envoy")
			c.Dir = sidecar_dir
			no_env_run(c, sidecar_dir, false, "docker-compose")
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

func read_env(name string, overrides []string) []string {
	envFile, err := os.ReadFile(name)
	if err != nil {
		fmt.Println("Error reading .env file:", err)
		cancel()
		panic(err)
	}

	overrideMap := make(map[string]string)
	for _, o := range overrides {
		parts := strings.SplitN(o, "=", 2)
		if len(parts) == 2 {
			overrideMap[parts[0]] = parts[1]
		}
	}

	if len(overrideMap) > 0 {
		fmt.Println("Applying overrides:", overrideMap)
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

		parts := strings.SplitN(line, "=", 2)
		key := parts[0]
		if _, ok := overrideMap[key]; ok {
			continue // skip overridden keys
		}

		envs = append(envs, line)
	}

	for k, v := range overrideMap {
		envs = append(envs, fmt.Sprintf("%s=%s", k, v))
	}

	// log the environment variables
	for _, env := range envs {
		fmt.Println("Environment variable:", env)
	}
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

func env_run(c *exec.Cmd, dir, env string, overrides []string) {
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
	c.Env = read_env(env, overrides)
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
