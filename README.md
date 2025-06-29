# Tansive

**Open platform for Policy-Driven, Auditable, Secure AI Agents**

Tansive is a developer- and ops-friendly platform for building, executing, and governing AI agents and tools with declarative workflows and fine-grained policies.

Whether you are automating tasks that touch sensitive systems, creating AI agents that securely access multiple systems to gather precise context, or building new workflows on top of existing data, Tansive provides the foundation to deploy and run them safely while meeting compliance requirements.

Tansive is not another agent framework. It's agnostic to frameworks and languages. Bring your own agents and tools—written in any language, using any interface, and Tansive will chain them together, enforce fine-grained policies, and manage their lifecycle.

---

## ✨ Key Features

- **Declarative Agent Catalog**  
  Hierarchically structured repository of agents, tools, and contextual data, partitioned across environments like dev, stage, and prod, and namespaces across teams or components.

- **Runtime Policy Enforcement**  
  Enforce fine-grained controls over access, execution, and data flows. Every agent and tool invocation is checked against policies in real time.

- **Immutable Constraints and Transforms**  
  Pin runtime sessions to specific values and apply user-defined transforms to filter or modify input securely.

- **Tamper-Evident Audit Logging**  
  Hash-linked, signed logs of every action for observability, compliance, and forensic analysis.

- **Language and Framework Agnostic**  
  Author tools and agents in any language—Python, Bash, Go, Node.js - no mandatory SDKs or special runtime servers.

- **GitOps Friendly**  
  Configure everything via declarative YAML specs version-controlled in Git, modeled on familiar cloud-native patterns.

---

## 🚀 Getting Started

Read detailed Intallation and Getting Started walkthrough at [docs.tansive.io](https://docs.tansive.io/getting-started)

> **Note:** Tansive is currently in **0.1-alpha** and rapidly evolving. Expect rough edges — feedback is welcome!

1. **Run the Tansive Server and Tangent**

```bash
cd scripts/docker
docker compose -f docker-compose-aio.yaml up -d
```

Wait for services to start.

2. **Install the CLI**

   Download the appropriate release binary named `tansive` from [Releases](https://github.com/tansive/tansive/releases) or build from source.

```bash
# Verify CLI installation
tansive version

# Configure CLI
tansive config --server https://local.tansive.dev:8678

# Login in single user mode
tansive login

# Verify status
tansive status
```

3. **Initialize a Catalog**

```bash
tansive create -f example_setup/catalog.yaml

# adopt view of the catalog
tansive set-catalog demo-catalog
```

4. **Setup Variants and Namespaces**

```bash
tansive create -f example_setup/catalog-setup.yaml
```

5. **Install SkillSets**

   Create a .env file in project root with your OpenAI or Anthropic API Key. Replace the placeholder with your key and leave the placeholder as it is.

```bash
# create a .env file with the following contents
CLAUDE_API_KEY="<your-key-here>"
OPENAI_API_KEY="<your-key-here>"
# don't modify this. you don't need a kubernetes cluster!
KUBECONFIG="YXBpVmVyc2lvbjogdjEKa2luZDogQ29uZmlnCmNsdXN0ZXJzOgogIC0gbmFtZTogbXktY2x1c3RlcgogICAgY2x1c3RlcjoKICAgICAgc2VydmVyOiBodHRwczovL2Rldi1lbnYuZXhhbXBsZS5jb20KICAgICAgY2VydGlmaWNhdGUtYXV0aG9yaXR5LWRhdGE6IDxiYXNlNjQtZW5jb2RlZC1jYS1jZXJ0Pg=="
```

```bash
# install the kubernetes troubleshooter
tansive create -f example_setup/skillset-k8s.yaml --variant dev
tansive create -f example_setup/skillset-k8s.yaml --variant prod

# install the health bot
tansive create -f example_setup/skillset-patient.yaml --variant dev
```

6. **Run the Troubleshooter Skill**

   Change `model` to "gpt4o" or "claude" depending on the API Key

```bash
# Run the Skill in 'dev' environment
tansive session create /demo-skillsets/kubernetes-demo/k8s_troubleshooter \
--view dev-view \
--input-args '{"prompt":"An order-placement issue is affecting our e-commerce system. Use the provided tools to identify the root cause and take any necessary steps to resolve it.","model":"claude"}'

# Run the Skill in 'prod' environment. Tansive policy will block redeployment in prod
tansive session create /demo-skillsets/kubernetes-demo/k8s_troubleshooter \
--view prod-view \
--input-args '{"prompt":"An order-placement issue is affecting our e-commerce system. Use the provided tools to identify the root cause and take any necessary steps to resolve it.","model":"claude"}'
```

7.  **Run the Health Bot Skill**

```bash
# Run the Health Bot with Session pinned to John's Patient Id
tansive session create /demo-skillsets/health-record-demo/health-record-agent --view dev-view --input-args '{"prompt":"John was looking sick. Can you please check her bloodwork?","model":"gpt4o"}'  --session-vars '{"patient_id":"H12345"}'

# Ask to fetch Sheila's record. Tansive will reject the bot's request
tansive session create /demo-skillsets/health-record-demo/health-record-agent --view dev-view --input-args '{"prompt":"Sheila was looking sick. Can you please check her bloodwork?","model":"gpt4o"}'  --session-vars '{"patient_id":"H12345"}'

```

## 📄 Documentation

Full documentation and examples are available at [https://docs.tansive.io](https://docs.tansive.io)

## 💬 Community and Support

Questions, Feedback, Ideas?

Reach out:

- [Discord](https://discord.gg/DARNwnyUhw)
- [X](https://x.com/gettansive)
- [LinkedIn](https://linkedin.com/company/tansive)

## 💼 License

Tansive is Open Source under the Apache 2.0 License

## 🙏 Contributing

Contributions, issues, and feature requests are welcome.
Please see CONTRIBUTING.md for guidelines.
