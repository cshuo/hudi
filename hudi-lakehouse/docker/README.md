<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Docker installation

Start a local Hudi lakehouse with Docker Engine / Docker Desktop and Compose
2.27.2 or newer (required for `docker compose config --environment`). The installer
checks this version before contacting the daemon or starting services.
Allocate at least 4 CPUs and 8 GB RAM to Docker. The first image
build needs internet access and several GB of disk space. The host needs no Java,
Maven, Python, Kubernetes, Helm or kubectl.

From a repository checkout:

```bash
./hudi-lakehouse/docker/lakehouse.sh --build --mcp-only
```

This builds the existing Spark Connect and Gateway Dockerfiles, starts MinIO,
Derby-backed Hive Metastore, Spark Connect and Gateway, loads sample Hudi tables,
and verifies the real MCP query path. `--mcp-only` requires no LLM or API key:
your external MCP client supplies its own model. The built-in chat UI needs an LLM.

| Interface | Default address |
|---|---|
| Chat UI | http://localhost:8000/ui/ |
| MCP (Streamable HTTP) | http://localhost:8000/mcp/ |
| Spark Connect | sc://localhost:15002 |
| MinIO console | http://localhost:9001 (admin / password) |

```bash
claude mcp add --transport http hudi http://localhost:8000/mcp/
```

All published ports bind to `127.0.0.1`. This is a local demo with fixed storage
credentials and unauthenticated services. HMS and the MinIO S3 API are internal
to the Compose network. Remote hosted MCP clients cannot reach localhost.

## Built-in chat

Choose one of these configurations, then open the Chat UI:

**Host Ollama:** pull `qwen3:8b` with your host Ollama installation, then run:

```bash
./hudi-lakehouse/docker/lakehouse.sh --build
```

The default endpoint is `http://host.docker.internal:11434`; the Compose host-gateway
mapping also supports Linux. On Linux, Ollama must listen on an interface reachable
from containers. An inaccessible service or missing model fails the installer's
chat readiness check with instructions, while keeping the lakehouse running.

**Containerized Ollama:** no host model installation is required:

```bash
./hudi-lakehouse/docker/lakehouse.sh --build --local-llm
```

This enables the `local-llm` profile, downloads `qwen3:8b`, and keeps models in a
named volume. Allow additional RAM/disk and time for the model download. This
profile uses CPU inference by default; it may be slow, especially on Docker Desktop.
Use the same flag on subsequent `up` calls to keep the Gateway pointed at this Ollama.
The installer checks for the configured model locally and only pulls it when missing,
so a cached installation can restart without contacting the model registry. Model
commands run inside the existing Ollama container. To refresh an installed model
explicitly while Ollama is running, use your configured model name in this command:

```bash
docker compose -f hudi-lakehouse/docker/compose.yaml --profile local-llm exec ollama ollama pull qwen3:8b
```

**API provider:** export your key and model, then start the stack:

```bash
export OPENAI_API_KEY=...  # or ANTHROPIC_API_KEY
export GATEWAY_LLM_MODEL=...  # a model available to your provider/account
./hudi-lakehouse/docker/lakehouse.sh --build
```

Without a `.env` or explicit provider, the script selects Anthropic when
`ANTHROPIC_API_KEY` is set, then OpenAI when `OPENAI_API_KEY` is set, otherwise
host Ollama. Keys are passed to the container and are never written by the installer.

For persistent configuration, copy `.env.example` to `.env` in this directory and
set `GATEWAY_LLM_PROVIDER`, `GATEWAY_LLM_MODEL` and the corresponding key there.
`.env` is gitignored; shell variables override it. An OpenAI-compatible service uses
`GATEWAY_LLM_PROVIDER=openai-compatible`, `GATEWAY_OPENAI_BASE_URL`,
`GATEWAY_LLM_MODEL` and, if required, `OPENAI_API_KEY`.
For chat installations, non-Ollama providers must have an explicit model. The
installer validates the values resolved by Compose, including `.env` interpolation
and shell overrides, before building or starting services. Only this directory's
`.env` is loaded; a `.env` in the caller's working directory is ignored.

`/health` checks Gateway liveness; `/ready` checks SQL and the configured LLM.
The installer verifies MCP independently and checks `/ready` plus local model
availability unless `--mcp-only` is selected. Model availability is checked against
the provider's own API (`/api/tags` for Ollama, `/models` for OpenAI-compatible),
with authentication when configured. Missing models and discovery failures fail
this check; the Gateway UI's fallback model list is not used.
For hosted API providers, readiness
only confirms that a key is configured; it does not validate the key or make a paid
inference call. `--mcp-only` skips LLM validation; it does not disable the chat routes.

## Sample data and catalog

The existing catalog path is preserved:

```text
Chat / MCP -> Gateway tools -> Spark Connect -> spark_catalog (HoodieCatalog)
                                                   | metadata: Hive Metastore
                                                   | Hudi data: MinIO
```

The seed container uses direct Spark Connect SQL to create `default.trips` (9 rows,
partitioned by `trip_date`) and `default.drivers` (3 rows). The Gateway's existing
`list_tables`, `describe_table` and `query_lakehouse` tools stay read-only.

Try: “List the tables”, “Describe trips”, or “For completed trips, sum fare_amount
plus tip_amount by city”. The last question should return:

| city | total |
|---|---:|
| chennai | 33.00 |
| san_francisco | 55.00 |
| sao_paulo | 77.00 |

Dates are yesterday at the first seed. Stable record keys and inserts filtered against existing keys
make retries safe without overwriting existing demo rows. The seed verifies counts,
partitions and totals before the Gateway starts. Reserve these demo table names:
existing incompatible schemas or modified sample data fail validation rather than
being dropped or reset. No semantic-layer service or catalog abstraction is added.

## Operations and persistence

`conf/hive-site.xml` is shared by schema initialization and the HMS server, including
the persistent Derby database path and S3A settings for HMS access to MinIO.
`conf/spark-defaults.conf` configures Spark Connect, its HMS client and Hudi storage access.

```bash
./hudi-lakehouse/docker/lakehouse.sh status
./hudi-lakehouse/docker/lakehouse.sh logs
./hudi-lakehouse/docker/lakehouse.sh test
./hudi-lakehouse/docker/lakehouse.sh down
```

`up` verifies sample data in the seed job before starting the Gateway, then checks
MCP and (unless skipped) chat readiness. `test` independently checks stored data and
calls the running Gateway over MCP without reseeding or requiring an LLM. `down`
removes containers and the network, preserving MinIO, HMS and Ollama volumes. Starting again with the same Compose project reuses them.
HMS is single-process Derby, matching the existing local-dev deployment; its image
runs under amd64 emulation on ARM hosts. Spark runs one shared `local[2]` driver.
If HMS initialization completed before its marker was saved, the next start validates
the existing Derby schema and restores the marker. A partial or invalid database is
preserved and startup stops with repair/restore guidance; it is never reinitialized
in place.

To verify persistence without a seed masking data loss:

```bash
docker compose -f hudi-lakehouse/docker/compose.yaml up -d --force-recreate --wait spark-connect
./hudi-lakehouse/docker/lakehouse.sh test
```

To intentionally **delete this installation's data**, run:

```bash
docker compose -f hudi-lakehouse/docker/compose.yaml --profile local-llm down --volumes
```

Use `COMPOSE_PROJECT_NAME` and the three port settings in `.env.example` for separate
installations. Preserve the same project name across `up`, `test` and `down` calls.
Do not run `docker compose config` in shared logs when API keys are configured:
Compose renders container environment values.

## Release image path

The base `compose.yaml` consumes images only; source builds are isolated in
`compose.build.yaml` and enabled by `--build`. Once images are available in a
release registry, set `SPARK_CONNECT_IMAGE` and `GATEWAY_IMAGE` to their published
references in `.env`, then run `lakehouse.sh` without `--build`. Compose pulls
missing images. The seed reuses the exact Connect image and its pinned Python
client instead of maintaining another Spark runtime.

Current default image names match the Helm charts and local build scripts. They
are **local build tags, not a claim that ASF publishes those images**. The RFC's
public registry namespace, versioned release downloads and `hudi.apache.org/lakehouse.sh`
bootstrap require release infrastructure decisions. This checkout provides the
Compose installation and contributor build path; it does not publish those endpoints.
Optional BI/Trino profiles, semantic models and minikube changes are separate work.

## Validation

Offline installer and Compose contract checks (Python standard library + Compose CLI):

```bash
python3 -m unittest discover -s hudi-lakehouse/tests -p 'test_docker_*.py' -v
```

The opt-in `Hudi Lakehouse Docker live acceptance` GitHub Actions workflow builds
the images, starts a clean installation, checks MCP, recreates containers without
reseeding to check persistence, and repeats installation to check seed idempotency.
It requires no LLM credentials. A real model chat can be tested separately through
the UI using the sample question above.
