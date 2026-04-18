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
	ctx, cancel := context.WithTimeout(k.tb.Context(), time.Minute*3)
	defer cancel()

	fullArgs := append(args, "--namespace", env.namespace, "--kubeconfig", env.kubeconfig)
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

func (k *kubectl) printClusterInfo(verbose bool) {
	k.tb.Helper()
	k.tb.Logf("\ncluster-info:%s\n", k.run("cluster-info"))
	if verbose {
		k.tb.Logf("\ncluster-info dump:%s\n", k.run("cluster-info", "dump"))
	}
	for _, name := range k.getPodNames() {
		k.tb.Logf("\n%s\n", k.run("describe", "pod", name))
	}
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

func (k *kubectl) getPodID(name string) string {
	k.tb.Helper()
	out := k.run("get", "pod", name, "-o", "jsonpath={.metadata.uid}")
	return strings.TrimSpace(out)
}

func (k *kubectl) deletePod(name string) {
	k.tb.Helper()
	k.run("delete", "pod", name, "--wait=false")
	k.tb.Logf("pod %s deleted", name)
}

func (k *kubectl) waitPodReplaced(name string, oldID string, timeout time.Duration) {
	k.tb.Helper()
	k.tb.Logf("waitPodReplaced: waiting for pod %s to disappear", name)
	require.Eventually(k.tb, func() bool {
		out, err := k.tryRun(
			"get", "pod", name,
			"-o", `jsonpath={.metadata.uid} {.status.conditions[?(@.type=="Ready")].status}`,
		)
		if err != nil {
			return false // pod doesnt exist yet
		}
		parts := strings.Fields(strings.TrimSpace(out))
		if len(parts) < 2 {
			return false // object hasnt been populated
		}

		nextID, isReady := parts[0], parts[1]
		return nextID != "" && nextID != oldID && isReady == "True"
	}, timeout, timeout/10, "waitPodReplaced: pod %s did not disappear in %s", name, timeout)

	k.tb.Logf("waitPodReplaced: pod %s dissappeared", name)
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

func (k *kubectl) requireClusterReady() {
	k.waitPodsReady(60 * time.Second)
	pods := k.getPodNames()
	require.Len(k.tb, pods, env.clusterSize, "requireClusterReady: expected %d pods, got %d", env.clusterSize, len(pods))
	require.True(k.tb, k.arePodsReady(), "requireClusterReady: not all pods are ready")
	k.tb.Logf("requireClusterReady: pods: %v", pods)
}
