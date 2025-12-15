package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"log/slog"
	"net"
	"test/utils"
)

var (
	serviceName    string
	listenPort     int
	downstreamURL  string
	busyLoopMicros int
	udpPort        int
	udpPeer        string
)

func main() {
	serviceName = os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		fmt.Println("SERVICE_NAME env var is required")
		os.Exit(1)
	}

	listenPort = utils.StrToInt(utils.GetEnvVar(serviceName+"_PORT", true))
	downstreamURL = utils.GetEnvVar(serviceName+"_DOWNSTREAM", false)
	busyLoopStr := utils.GetEnvVar(serviceName+"_BUSY_LOOP", false)
	if busyLoopStr != "" {
		busyLoopMicros = utils.StrToInt(busyLoopStr)
	}
	// if busyLoop is not set, default to 0

	udpPortStr := utils.GetEnvVar(serviceName+"_UDP_PORT", false)
	if udpPortStr != "" {
		udpPort = utils.StrToInt(udpPortStr)
	}
	udpPeer = utils.GetEnvVar(serviceName+"_UDP_PEER", false)

	logger := utils.GetLogger(serviceName)
	logger.Info("Starting server", "port", listenPort, "downstream", downstreamURL, "busy_loop", busyLoopMicros)

	if udpPort > 0 {
		go startUDPServer(udpPort, logger)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		//start := time.Now()

		// Propagate rpc-id
		//rpcID := r.Header.Get("rpc-id")
		//if rpcID == "" {
		//	rpcID = "unknown"
		//}

		//logger.Info("Received request", "rpc-id", rpcID)

		if udpPeer != "" {
			err := sendUDPPing(udpPeer, logger)
			if err != nil {
				logger.Error("UDP ping failed", "error", err)
			}
		}

		// Pre-processing busy loop (half of total)
		if busyLoopMicros > 0 {
			busyLoop(busyLoopMicros / 2)
		}

		// Call downstream if configured
		var downstreamResp string
		if downstreamURL != "" {
			req, err := http.NewRequest("GET", downstreamURL, nil)
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to create request: %v", err), http.StatusInternalServerError)
				return
			}
			//req.Header.Set("rpc-id", rpcID)

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to call downstream: %v", err), http.StatusInternalServerError)
				return
			}
			defer resp.Body.Close()

			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to read downstream response: %v", err), http.StatusInternalServerError)
				return
			}
			downstreamResp = string(bodyBytes)
		} else {
			downstreamResp = "leaf"
		}

		// Post-processing busy loop (half of total)
		if busyLoopMicros > 0 {
			busyLoop(busyLoopMicros / 2)
		}

		//totalDuration := time.Since(start)
		//logger.Info("Request completed", "rpc-id", rpcID, "duration", totalDuration)

		//w.Header().Set("rpc-id", rpcID)
		fmt.Fprintf(w, "%s -> %s", serviceName, downstreamResp)
	})

	err := http.ListenAndServe(fmt.Sprintf(":%d", listenPort), nil)
	if err != nil {
		logger.Error("Server failed", "error", err)
	}
}

func busyLoop(repeat int) {
	for range repeat {
		for range 10000 {
		}
	}
}

func busyLoopDuration(duration_us int) {
	spent_us := 0
	var now time.Time
	for spent_us < duration_us {
		now = time.Now()
		// Calibration: run a small loop and measure
		busyLoop(1)
		spent_us += int(time.Since(now).Microseconds())
	}
}

func startUDPServer(port int, logger *slog.Logger) {
	conn, err := net.ListenPacket("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Error("Failed to start UDP server", "error", err)
		return
	}
	defer conn.Close()
	logger.Info("UDP server started", "port", port)

	buf := make([]byte, 1024)
	for {
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			logger.Error("UDP read error", "error", err)
			continue
		}
		//		logger.Info("UDP received", "data", string(buf[:n]), "from", addr)

		_, err = conn.WriteTo(buf[:n], addr)
		if err != nil {
			logger.Error("UDP write error", "error", err)
		}
	}
}

func sendUDPPing(peer string, logger *slog.Logger) error {
	conn, err := net.Dial("udp", peer)
	if err != nil {
		return err
	}
	defer conn.Close()

	//logger.Info("Sending UDP ping", "peer", peer)
	_, err = conn.Write([]byte("ping"))
	if err != nil {
		return err
	}

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.Read(buf)
	if err != nil {
		return err
	}
	//logger.Info("Received UDP pong", "data", string(buf[:n]))
	return nil
}
