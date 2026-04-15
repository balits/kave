package smoke

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type kubectl struct {
	tb testing.TB
}

func k8sClient(tb testing.TB) *kubectl {
	return &kubectl{tb: tb}
}

func (k *kubectl) run(args ...string) string {
	out, err := k.tryRun(args...)
	require.NoError(k.tb, err)
	return out
}

func (k *kubectl) tryRun(args ...string) (string, error) {
	// k.tb.Helper()
	ctx, cancel := context.WithTimeout(k.tb.Context(), time.Second*10)
	defer cancel()

	fullArgs := append(args, "--namespace ", env.namespace)
	cmd := exec.CommandContext(ctx, "kubectl", fullArgs...)
	var (
		stdout bytes.Buffer
		stderr bytes.Buffer
	)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		return stdout.String(), fmt.Errorf("failed to run kubectl %s\nstderr: %s", strings.Join(fullArgs, " "), stderr.String())
	}
	return stdout.String(), nil
}

func (k *kubectl) getPodNames() []string {
	k.tb.Helper()
	out := k.run("get", "pods",
		"-l", "app.kubernetes.io/name=kave,kave/role=voter",
		"-o", "jsonpath={.items[*].metadata.name}",
	)
	out = strings.TrimSpace(out)
	if out == "" {
		return nil
	}
	return strings.Split(out, " ")
}

func (k *kubectl) deletePod(name string) {
	k.tb.Helper()
	k.run("delete", "pod", name, "--wait=false")
	k.tb.Logf("pod %s deleted", name)
}

func (k *kubectl) arePodsReady() bool {
	k.tb.Helper()
	out := k.run("get", "pods",
		"-l", "app.kubernetes.io/name=kave,kave/role=voter",
		"-o", "json",
	)

	// ugly struct hehe
	var podList struct {
		Items []struct {
			Status struct {
				Conditions []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
			} `json:"status"`
		} `json:"items"`
	}
	require.NoError(k.tb, json.Unmarshal([]byte(out), &podList))

	if len(podList.Items) == 0 {
		return false
	}

	for _, pod := range podList.Items {
		ready := false
		for _, c := range pod.Status.Conditions {
			if c.Type == "Ready" && c.Status == "True" {
				ready = true
				break
			}
		}
		if !ready {
			return false
		}
	}
	return true
}

func (k *kubectl) waitPodsReady(timeout time.Duration) {
	k.tb.Helper()
	k.run("wait", "pod",
		"-l", "app.kubernetes.io/name=kave,kave/role=voter",
		"--for=condition=Ready",
		fmt.Sprintf("--timeout=%ds", int(timeout.Seconds())),
	)
}

func (k *kubectl) rolloutRestart() {
	k.tb.Helper()
	k.run("rollout", "restart", "statefulset/kave-voter")
	k.tb.Log("triggered rollout restart")
}

func (k *kubectl) waitRollout(timeout time.Duration) {
	k.tb.Helper()
	k.run("rollout", "status", "statefulset/kave-voter",
		fmt.Sprintf("--timeout=%ds", int(timeout.Seconds())),
	)
}
