package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// proc is one supervised Kosh process. Killing one and watching the system
// cope is the point, so the lab leaves a dead process down for a while
// (LAB_HEAL_AFTER) before it restarts it, so the demo is whole again for the
// next visitor.
type proc struct {
	name string

	mu        sync.Mutex
	env       []string
	cmd       *exec.Cmd
	state     string // running | stopped | crashed
	pid       int
	started   time.Time
	downSince time.Time // when it stopped or crashed; zero while running
	restarts  int
	lastExit  string
	logs      []string // last 200 lines
	stopAsked bool
}

type procView struct {
	Name     string    `json:"name"`
	State    string    `json:"state"`
	PID      int       `json:"pid"`
	Started  time.Time `json:"started_at"`
	Restarts int       `json:"restarts"`
	LastExit string    `json:"last_exit"`
	Logs     []string  `json:"logs"`
}

func binPath(dir, name string) string {
	p := filepath.Join(dir, name)
	if runtime.GOOS == "windows" {
		p += ".exe"
	}
	return p
}

func (p *proc) start(dir string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == "running" {
		return nil
	}
	cmd := exec.Command(binPath(dir, p.name))
	cmd.Env = append(os.Environ(), p.env...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		p.state, p.lastExit, p.downSince = "crashed", err.Error(), time.Now()
		return fmt.Errorf("start %s: %w", p.name, err)
	}
	if !p.started.IsZero() {
		p.restarts++
	}
	p.cmd, p.state, p.pid, p.started, p.stopAsked = cmd, "running", cmd.Process.Pid, time.Now(), false
	p.downSince = time.Time{}
	go p.capture(out)
	go p.wait(cmd)
	return nil
}

func (p *proc) capture(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		p.mu.Lock()
		p.logs = append(p.logs, sc.Text())
		if len(p.logs) > 200 {
			p.logs = p.logs[len(p.logs)-200:]
		}
		p.mu.Unlock()
	}
}

func (p *proc) wait(cmd *exec.Cmd) {
	err := cmd.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != cmd {
		return
	}
	p.pid, p.downSince = 0, time.Now()
	if p.stopAsked {
		p.state, p.lastExit = "stopped", "killed by the lab"
		return
	}
	p.state = "crashed"
	if err != nil {
		p.lastExit = err.Error()
	} else {
		p.lastExit = "exited"
	}
}

// kill sends SIGKILL (TerminateProcess on Windows): no shutdown hooks, no
// flushes, exactly like a crash.
func (p *proc) kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state != "running" || p.cmd == nil {
		return nil
	}
	p.stopAsked = true
	return p.cmd.Process.Kill()
}

// setEnv replaces KEY=... in the environment the next start uses.
func (p *proc) setEnv(key, value string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	kept := p.env[:0:0]
	for _, kv := range p.env {
		if !strings.HasPrefix(kv, key+"=") {
			kept = append(kept, kv)
		}
	}
	p.env = append(kept, key+"="+value)
}

// down reports whether the process is not running, and since when.
func (p *proc) down() (bool, time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state != "running", p.downSince
}

func (p *proc) view(withLogs bool) procView {
	p.mu.Lock()
	defer p.mu.Unlock()
	v := procView{Name: p.name, State: p.state, PID: p.pid, Started: p.started, Restarts: p.restarts, LastExit: p.lastExit}
	if withLogs {
		n := len(p.logs)
		v.Logs = append([]string(nil), p.logs[max(0, n-30):]...)
	}
	return v
}
