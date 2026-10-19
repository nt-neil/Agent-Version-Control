package govern

import (
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

// loadAvg returns the 1-minute load average.
func loadAvg() (float64, bool) {
	var s string
	if runtime.GOOS == "linux" {
		b, err := os.ReadFile("/proc/loadavg")
		if err != nil {
			return 0, false
		}
		s = string(b)
	} else {
		b, err := exec.Command("sysctl", "-n", "vm.loadavg").Output()
		if err != nil {
			return 0, false
		}
		s = strings.Trim(strings.TrimSpace(string(b)), "{} ")
	}
	f := strings.Fields(s)
	if len(f) == 0 {
		return 0, false
	}
	v, err := strconv.ParseFloat(f[0], 64)
	return v, err == nil
}

var vmLine = regexp.MustCompile(`Pages (free|inactive|speculative):\s+(\d+)`)
var pageSize = regexp.MustCompile(`page size of (\d+) bytes`)

// availableMB estimates memory that can be handed out without swapping.
func availableMB() (int, bool) {
	if runtime.GOOS == "linux" {
		b, err := os.ReadFile("/proc/meminfo")
		if err != nil {
			return 0, false
		}
		for _, line := range strings.Split(string(b), "\n") {
			if f := strings.Fields(line); len(f) >= 2 && f[0] == "MemAvailable:" {
				kb, err := strconv.Atoi(f[1])
				return kb / 1024, err == nil
			}
		}
		return 0, false
	}
	b, err := exec.Command("vm_stat").Output()
	if err != nil {
		return 0, false
	}
	ps := 4096
	if m := pageSize.FindSubmatch(b); m != nil {
		ps, _ = strconv.Atoi(string(m[1]))
	}
	var pages int
	for _, m := range vmLine.FindAllSubmatch(b, -1) {
		n, _ := strconv.Atoi(string(m[2]))
		pages += n
	}
	if pages == 0 {
		return 0, false
	}
	return pages * ps / (1 << 20), true
}
