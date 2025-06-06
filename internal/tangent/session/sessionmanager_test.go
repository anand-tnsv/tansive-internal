// cursor: disable-autocomplete
// @copilot-disable
package session

const skillSet = `
version: v1
kind: SkillSet
metadata:
  name: kubernetes-demo
  variant: example-variant
  path: /skillsets
spec:
  version: "0.1.0"
  runner:
    id: "system.commandrunner"
    config:
      command: "bash"
      args:
        - "-c"
      env:
        - key: "some-key"
          value: "some-value"
      exec: "my-bash-script.sh"
      security:
        - type: default #could be one of: default, TEZ
  context:
    - name: customer-cache
      schema:
        type: object
        properties:
          kubeconfig:
            type: string
            format: binary
        required:
          - kubeconfig
      value:
        kubeconfig: YXBpVmVyc2lvbjogdjEKa2luZDogQ29uZmlnCmNsdXN0ZXJzOgogIC0gbmFtZTogbXktY2x1c3RlcgogICAgY2x1c3RlcjoKICAgICAgc2VydmVyOiBodHRwczovL215LWNsdXN0ZXIuZXhhbXBsZS5jb20KICAgICAgY2VydGlmaWNhdGUtYXV0aG9yaXR5LWRhdGE6IDxiYXNlNjQtZW5jb2RlZC1jYS1jZXJ0Pg==
      annotations:
  skills:
    - name: list_pods
      description: "List pods in the cluster"
      inputSchema:
        type: object
        properties:
          labelSelector:
            type: string
            description: "Kubernetes label selector to filter pods"
        required:
          - labelSelector
      outputSchema:
        type: string
        description: "Raw output from listing pods, typically from 'kubectl get pods'"
      exportedActions:
        - kubernetes.pods.list
      annotations:
        llm:description: |
          Lists all pods in the currently active Kubernetes cluster. This skill supports an optional labelSelector argument to filter pods by label. It is a read-only operation that provides visibility into running or failing workloads. The output is a plain-text summary similar to kubectl get pods. Use this to diagnose the current state of the system.
    - name: restart_deployment
      description: "Restart a deployment"
      inputSchema:
        type: object
        properties:
          deployment:
            type: string
        required:
          - deployment
      outputSchema:
        type: string
        description: "Raw output from restarting the deployment, typically from 'kubectl rollout restart deployment <deployment>'"
      exportedActions:
        - kubernetes.deployments.restart
      annotations:
        llm:description: |
          Performs a rollout restart of a Kubernetes deployment. This skill is used to trigger a fresh rollout of pods associated with a deployment, typically to recover from failures or apply configuration changes. It requires the deployment name as input and will execute the equivalent of kubectl rollout restart deployment <name>.
`
