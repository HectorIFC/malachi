# Malachi → NorthGuard: design doc de port open-source em Elixir

> Status: **proposta** · Estratégia escolhida: **B faseado** (Elixir puro → Rust NIF só onde o profiling exigir)
> Decisão de viabilidade registrada em [Viabilidade](#1-viabilidade-medida) (benchmark em `benchmark/storage_viability.exs`).

Objetivo: reimplementar a **arquitetura** do [NorthGuard](https://www.linkedin.com/blog/engineering/infrastructure/introducing-northguard-and-xinfra)
(log storage escalável da LinkedIn) como projeto **open source em Elixir**, partindo do
malachi atual (broker TCP 100% in-memory). Não buscamos a *escala* da LinkedIn (PB/dia em
10k brokers); buscamos a *fidelidade de design* com performance adequada para escala
pequena/média e um caminho claro de evolução.

---

## 1. Viabilidade (medida)

Benchmark de I/O do BEAM puro no caminho crítico do storage (Apple M1, OTP 28 JIT, SSD NVMe).
Reproduza com `elixir benchmark/storage_viability.exs`.

| Cenário (Elixir puro, `:file` raw) | Throughput | Latência/flush |
|---|---|---|
| Durável fsync/batch, 10MB (alvo NG) | 472 MB/s · 484k rec/s | p50 **8.0ms** · p99 92ms |
| Durável, 20k×256B (count-driven) | 1234 MB/s · 5M rec/s | p50 3.4ms · p99 12.7ms |
| Não-durável (teto) | 612–1507 MB/s | — |
| Leitura sequencial | 2487 MB/s | — |

**Alvo real do NorthGuard:** ~20 MB/s de escrita por broker (steady-state), ~60 MB/s com
replicação 3×. O Elixir puro entrega **~10–50× isso** num laptop. **Throughput não é o gargalo.**

**Veredito:** Fase 0/1 em Elixir puro é viável e performática. O Rust NIF (Fase 2) se justifica
por três motivos — e nenhum deles é throughput:
1. **Cauda de latência** (vistos picos de até ~1.5s no max sob flush grande; pode piorar sob
   concorrência real de milhares de conexões + N segments com fsync simultâneo).
2. **`O_DIRECT`** — NorthGuard usa Direct I/O para evitar degradação do page cache em réplicas
   não-consumidas e leituras de segments antigos. BEAM puro não expõe O_DIRECT.
3. **Cache em nível de aplicação** alimentado pelos consume streams (idem item 2).

Caveat: no macOS `:file.sync` não faz `F_FULLFSYNC`; latências são otimistas vs. Linux server
(onde fsync é mais lento porém mais consistente). Throughput é representativo.

---

## 2. Gap analysis: malachi hoje vs. NorthGuard

O malachi já tem **a metade que o BEAM faz melhor que C++**. O que falta é a metade durável.

| Capacidade | malachi hoje | NorthGuard | Gap |
|---|---|---|---|
| Data plane TCP, conexões, sessões | ✅ `tcp_acceptor(_pool)`, `tcp_protocol`, `connection_registry` | sessionized streaming | **Adaptar** (pipelining/windowing) |
| Backpressure / flow control | ✅ `backpressure`, `rate_limiter`, `connection_limiter` | windowing por stream | **Adaptar** |
| Fila / partição em memória | ✅ `queue`, `partition_manager`, `consumer`, `ack_manager` | range/segment durável | **Substituir modelo** |
| **Persistência durável (log)** | ❌ tudo em memória | fps-store (WAL, file-per-segment, fsync) | **Construir** ← núcleo |
| **Modelo record→segment→range→topic** | ❌ (queue/partition) | ✅ | **Construir** |
| **Metadados sharded (DS-RSM/Raft)** | ❌ | ✅ vnodes + coordinators | **Construir** |
| **Membership gossip (SWIM)** | ❌ (`connection_registry` local) | ✅ | **Construir** |
| **Striping / self-balancing** | ❌ | ✅ segment como unidade de replicação | **Construir** |
| Auth / TLS / métricas / dashboard | ✅ `auth/*`, `tls_validator`, `metrics`, `dashboard` | (interno LinkedIn) | **Manter** |
| Storage policies / attributes | ❌ | ✅ expressões sobre attributes | **Construir** |

---

## 3. Arquitetura-alvo

### 3.1 Modelo de dados (igual ao NorthGuard)

```
Topic   ── coleção nomeada de Ranges que cobrem todo o keyspace
 └ Range  ── abstração de LOG: segments p/ uma faixa contígua de chaves (active|sealed)
    └ Segment ── UNIDADE DE REPLICAÇÃO: sequência de records (active|sealed; sela em 1GB / 1h / falha)
       └ Record ── key + value + headers (bytes), offset lógico no segment
```

- **Split/merge de range são operações puramente lógicas de metadados** — segments nunca são
  fisicamente combinados/copiados (confirmado no vídeo do meetup). Merge só entre *buddy ranges*
  (estilo buddy allocator).
- Ordenação total preservada via happens-before em splits/merges.

### 3.2 Camada de storage — `Malachi.SegmentStore` (behaviour pluggable)

NorthGuard diz que o storage é pluggable ("fps-store" é só a impl primária). Replicamos isso:

```elixir
@callback open(seg_id, opts) :: {:ok, handle} | {:error, term}
@callback append(handle, batch :: [record]) :: {:ok, base_offset} | {:error, term}
@callback sync(handle) :: :ok | {:error, term}        # fsync — chamado antes do ack
@callback read(handle, offset, max_bytes) :: {:ok, [record]} | :eof
@callback seal(handle) :: :ok                          # torna imutável
@callback sparse_index(handle, offset) :: {:ok, file_pos}
```

- **Fase 0/1:** `Malachi.SegmentStore.Elixir` — `:file` `[:raw, :binary]`, batching por
  10ms / N records / N bytes, fsync antes do ack, índice esparso em ETS/DETS ou arquivo `.idx`.
- **Fase 2 (condicional):** `Malachi.SegmentStore.Native` — Rust NIF (Rustler) com O_DIRECT,
  buffers alinhados, cache em nível de app; índice em `erlang-rocksdb`.

### 3.3 Metadados — DS-RSM com `ra`

- **vnode** = grupo Raft (`ra`) guardando um shard do metadado (topics/ranges/segments).
- **coordinator** = líder do vnode; carrega a "lógica de negócio" (sela/deleta topic,
  split/merge range, replica sets de segment, self-healing de segments sub-replicados).
- **DS-RSM** = vnodes sobre um hash ring (consistent hashing). Hash por nome do topic; por
  range ID para range/segment. **Posição do vnode no anel é estável** mesmo quando réplicas
  Raft entram/saem (detalhe do vídeo). **vnodes podem dar split** (quebra o estado em dois
  grupos Raft).

### 3.4 Membership — SWIM

- `partisan` (suporta SWIM) ou impl própria. Random probing p/ detecção + dissemination
  estilo infecção. Espalha **estado global mínimo**: host/port/attributes dos brokers +
  fronteiras/líder/term dos vnodes (para roteamento).

### 3.5 Protocolos

- **Metadados = unary** (1 req → 1 resp): create/delete/topicMetadata/segmentMetadata.
  Qualquer broker é proxy → roteia ao líder do vnode via estado gossipado.
- **Dados = streaming sessionizado** (produce/consume/replication) com pipelining + windowing.
  Reaproveita `tcp_protocol` + `backpressure` do malachi. Consume pode usar `:file.sendfile`.

### 3.6 Storage/metadata policies

- Policy = nome + retenção + constraints. Constraint = expressão sobre **attributes**
  (k/v opacos que admins ligam a brokers). Generaliza rack/DC-awareness sem o core entender
  "rack". Decide replica sets de segment e replicas de vnode.

---

## 4. Roadmap faseado

### Fase 0 — Persistência e modelo de log (Elixir puro)
- ✅ `Malachi.Storage.SegmentStore` behaviour + impl `Malachi.Storage.ElixirStore`
  (file-per-segment, batching, fsync-antes-do-ack, índice esparso, sealing, crash recovery,
  `open_read` barato p/ segments selados via `.idx`).
- ✅ Tipos `Malachi.Log.Record` (framing + CRC32) e `Malachi.Log.Segment` (offsets lógicos).
- ✅ `Malachi.Log` — log multi-segment: rolling/sealing automático (size/age), leitura
  contínua atravessando segments, recovery do diretório inteiro (só escaneia o último segment).
- ✅ Flush por **tamanho** (`:flush_bytes`, default 10MB — gatilho de tamanho do NorthGuard):
  `append` faz flush+fsync automático ao atingir o limite.
- ✅ Testes: property-based (`stream_data`) + unit de append/read/seal/crash-recovery/roll/auto-flush.
- ⏳ Pendente da Fase 0: tipos lógicos `Range`/`Topic`; flush por **tempo** (gatilho ~10ms) e por
  **contagem** (20k records) via um wrapper GenServer; scan de recovery em chunks (hoje lê o
  segment inteiro em memória).

### Fase 1 — Distribuição (Elixir puro)
- DS-RSM com `ra` (vnodes, coordinators, consistent hashing, split de vnode).
- SWIM (`partisan`) + estado global mínimo + roteamento de requests unários.
- **Striping**: segment como unidade de replicação; self-balancing ao criar segments.
- Replicação de segment (active stream + sealed = consume entre brokers) + self-healing.
- Storage/metadata policies + attributes.

### Fase 2 — Eficiência nativa (condicional, guiada por profiling)
- `Malachi.SegmentStore.Native` em Rust (Rustler): O_DIRECT, cache de app, `erlang-rocksdb`.
- Só implementar se a Fase 1 mostrar cauda de latência/page-cache como gargalo sob concorrência.

### (Futuro) Camada Xinfra-like
- Virtual topics com epochs, offsets opacos, migração dual-write, consumer-group management.
  Fora do escopo inicial.

---

## 5. Dependências candidatas

| Necessidade | Lib | Notas |
|---|---|---|
| Raft (DS-RSM/vnodes) | [`ra`](https://github.com/rabbitmq/ra) | Raft de produção do RabbitMQ |
| Membership SWIM/gossip | [`partisan`](https://github.com/lasp-lang/partisan) | ou impl própria |
| Índice esparso nativo (Fase 2) | [`erlang-rocksdb`](https://github.com/emqx/erlang-rocksdb) | binding RocksDB |
| NIF nativo (Fase 2) | [`rustler`](https://github.com/rusterlium/rustler) | NIFs Rust memory-safe + dirty schedulers |
| Testes de propriedade | `stream_data` / `PropEr` | substituto parcial da sim determinística |

---

## 6. O que NÃO vamos replicar (e o substituto)

**Simulação determinística** (cluster+clientes single-thread, swap de time/net/disk/RNG, replay
exato de falhas) — um dos pilares de confiabilidade do NorthGuard — **é essencialmente inviável
no BEAM**, porque não controlamos o scheduler (preemptivo, multicore). É um downgrade real de
garantias e precisa ser aceito explicitamente.

Substitutos:
- Property-based stateful testing (`stream_data`/`PropEr`) do modelo de log e da máquina de estados.
- `Concuerror` para checagem de concorrência (escala limitada).
- Testes estilo Jepsen para consistência distribuída.
- Fault injection via `partisan` (partições de rede) + chaos no storage (corrupção/erro de I/O).

---

## 7. Questões em aberto

1. Replicação na Fase 1: implementar do zero sobre `ra`/streams, ou apostar mais no `ra`?
2. Índice esparso na Fase 0: arquivo `.idx` próprio vs. ETS persistido vs. DETS?
3. Compatibilidade de protocolo: manter o protocolo TCP atual do malachi ou desenhar o
   sessionizado do NorthGuard desde a Fase 0?
4. Formato de offset: opaco (estilo Xinfra) desde já, ou `long` simples no início?
