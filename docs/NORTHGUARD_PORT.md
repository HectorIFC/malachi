# Malachi → NorthGuard: design doc de port open-source em Elixir

> Status: **proposta** · Estratégia escolhida: **B faseado** (Elixir puro → Rust NIF só onde o profiling exigir)
> Decisão de viabilidade registrada em [Viabilidade](#1-viabilidade-medida) (benchmark em `benchmark/storage_viability.exs`).

Objetivo: reimplementar a **arquitetura** do [NorthGuard](https://www.linkedin.com/blog/engineering/infrastructure/introducing-northguard-and-xinfra)
(log storage escalável da LinkedIn) como projeto **open source em Elixir**, partindo do
malachi atual (broker TCP 100% in-memory).

> **Objetivo evoluído (a partir de 2026-06-27):** com a arquitetura NorthGuard concluída e sem SPOF
> (control plane Raft multi-nó + data plane com replicação por quórum, self-healing, failover,
> catch-up e membership SWIM), o novo norte é tornar o **malachi um produto escalável e vendável,
> melhor que o Kafka open source**, usando os conceitos do NorthGuard. Hoje há **dois mundos
> desconectados**: o broker TCP in-memory vivo (modelo de filas/pub-sub, estilo RabbitMQ) e o stack
> NorthGuard (modelo de log, estilo Kafka). **A prioridade #1 é conectá-los** (expor o stack
> NorthGuard ao cliente — a "Fase 3" abaixo); escala (incl. sharding) deixa de ser fora do alvo.

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
- ✅ `Malachi.Range` — abstração de log sobre keyspace `[0, 2^bits)`: chaves por hash
  (`:erlang.phash2`), ranges como blocos buddy-allocator, **split/merge lógicos** (não movem
  dados), buddy único, linhagem (`parents`) para **happens-before**.
- ✅ `Malachi.Topic` — coleção nomeada de ranges que **cobrem todo o keyspace**: roteamento de
  records por chave (`route`/`append`), orquestração de split/merge mantendo a cobertura,
  sealing, ranges selados retidos para leitura. Metadados do topic são **em memória** por ora
  (persistência durável vem com o DS-RSM da Fase 1).
- ✅ Índice esparso em `:array` (busca binária O(log n), insert O(log n)).
- ✅ **Recovery em chunks** (`scan_segment`) — nunca carrega o segment inteiro na memória.
- ✅ **Flush por contagem** (`:flush_count`, default 20k) no `ElixirStore` + **flush por tempo**
  (~10ms) e **acesso concorrente serializado** no `Malachi.TopicServer` (GenServer que envolve o
  Topic, com timer de flush e flush no shutdown).
- ✅ **Leitura cross-epoch** (`Topic.read_history/2`) — histórico de uma chave através de um split
  (epoch do pai → epoch do filho), via linhagem + happens-before.
- ✅ Testes: property-based (`stream_data`) + unit — append/read/seal/crash-recovery/roll/
  auto-flush (size+count) + split/merge/buddy/happens-before + cobertura/roteamento/cross-epoch +
  flush por tempo e concorrência no TopicServer. **66 testes + 2 propriedades.**

> **Fase 0 concluída.** Único item adiado de propósito: **persistência durável dos metadados do
> topic** (quais ranges existem) — entra no **DS-RSM da Fase 1**, fiel ao NorthGuard, onde essa
> metadata vive no coordinator/vnode (Raft).

### Fase 1 — Distribuição

Estratégia confirmada: **lógica pura primeiro, `ra` depois** (mesmo padrão de
`ElixirStore`→Rust NIF). A máquina de estado do metadado é desenhada com o contrato
`apply/2` de um Raft, então o `ra` pluga sem retrabalho.

**Fase 1a — DS-RSM (lógica pura, sem deps):**
- ✅ `Malachi.Cluster.HashRing` — consistent hashing: vnodes em tokens no anel `[0, 2^bits)`,
  `route` por ceiling com wraparound, `boundaries` (arco do vnode), add/remove. Movimentação
  mínima ao adicionar/remover vnode (testado).
- ✅ `Malachi.Keyspace` — math de keyspace compartilhada (`size_for_bits!`, `position_of`,
  `within?`, `splittable?`, `split_point`, `buddies?`); `Range` e `HashRing` refatorados para
  usá-la (mata a duplicação de hash + validação `1..32`).
- ✅ `Malachi.Metadata` — máquina de estado do vnode/coordinator (`apply/2` **determinístico**,
  contrato de Raft): topics, ranges (split/merge buddy via `Keyspace`, linhagem) e segments
  (replica set, estado active/sealed). Ids de range vêm de um contador no estado (sem
  `unique_integer`/timestamp dentro do `apply` → seguro p/ replicação). **Resolve a persistência
  de metadados do topic adiada da Fase 0.** Testado inclusive com replay do log (determinismo) e
  com catch-all defensivo (comando desconhecido retorna erro, não derruba a réplica).
  - ✅ Índices secundários `%{topic => range_ids}` e `%{range_id => segment_ids}` — hoje
    `ranges_of_topic`/`segments_of_range` varrem **todos** os ranges/segments do vnode (O(n)), e estão no
    hot path (todo produce roteia por `active_ranges_of_topic`; todo consume lê `segments_of_range`), então
    o custo cresce com o total de metadado acumulado (retenção guarda selados). Fatiado:
    - ✅ **V-idx-a — manter + validar o índice.** `Metadata` ganha `topic_ranges`/`range_segments`
      (MapSets, entrada existe sse tem ≥1 membro), mantidos **dentro do `apply/2` determinístico** (não é
      cache lateral — replicado igual em todo nó `ra`) em cada mutação de membership: `create_topic`,
      `split_range`, `merge_ranges`, `register_segment` (+), `delete_segment`, `delete_topic` (−), e a
      migração `extract_topic`/`insert_topic`; seal/set_replicas/commit/policies não mexem. `DSRSM.merged_metadata`
      também une o índice (shards disjuntos por topic → `Map.merge` exato). **Nenhum leitor usa ainda** — os
      scans seguem intactos. Property test: o índice == um índice reconstruído por scan após qualquer
      sequência de comandos (pega faltante/extra/stale/vazio); a property de determinismo já cobre o índice
      (compara o state inteiro). 685 testes, 0 falhas; credo/dialyzer limpos.
    - ✅ **V-idx-b — trocar os leitores para o índice.** `ranges_of_topic`/`segments_of_range` viram lookup
      `Map.get(index) |> Enum.map(&Map.fetch!(...))` = **O(1)+O(k)** (o `fetch!` é seguro pelo invariante do
      V-idx-a, e serve de canário se algum dia divergir). Também index-based os scans internos por-topic:
      `seal_topic` (sela só os ranges do topic) e `delete_topic` (dropa só os ranges/segments do topic) —
      removidos os helpers de scan `seal_ranges_of_topic`/`range_ids_of_topic`(scan)/`drop_segments_in`. O
      caminho `DSRSM` usa o índice de graça (delega aos leitores do `Metadata` via `query`; o merge do
      índice no `merged_metadata` veio no V-idx-a). Pré-condição verificada: nenhum `%Metadata{}` é
      construído com dados sem índice (todos vêm de `apply`/merge/migração). Os leitores antigos já eram
      não-ordenados, então nenhum caller dependia da ordem — sem regressão. Validado pela suíte inteira
      (broker/DSRSM/produce/consume exercitam os leitores nos caminhos vivos) + as properties. 686 testes,
      0 falhas; credo/dialyzer limpos.
    - ✅ **V-idx-c — medir o ganho.** `bench/metadata_index_bench.exs` compara índice vs scan (reimplementado
      inline) no mesmo Metadata, com topics de tamanho fixo (3 ranges, 2 segments cada) e o total crescendo.
      Resultado: o índice é **plano** (~0,2 µs, O(k)) enquanto o scan cresce **linear** (O(n)) — de ~7,5 µs
      (300 ranges) a **~7,3 ms** (150k ranges) por chamada de `ranges_of_topic`, e a ~9,9 ms para
      `segments_of_range` (100k segments). Speedup de ~12× (pequeno) a **~33.000×** (ranges) / **~67.000×**
      (segments). Como isso era um imposto **por produce/consume**, a 50k topics o scan sozinho custava
      milissegundos por mensagem; o índice o zera. **Índice secundário (V-idx-a/b/c) completo.**
- ✅ `Malachi.Cluster.DSRSM` — junta tudo: HashRing + um `Metadata` por vnode; `command/3` e
  queries roteados por **nome do topic** ao vnode dono (sharding de topics entre vnodes, testado).
  Determinístico (replay). Decisão Fase 1a: metadado de um topic **co-localizado** num vnode
  (route por nome) — desvio anotado do range-id sharding do NorthGuard.
- ✅ **Split de vnode** (o "D" — dinâmico — do DS-RSM): `DSRSM.split_vnode/3` adiciona um vnode
  e **migra** os topics deslocados (topic + ranges + segments) para ele. Viabilizado por range id
  `{topic, seq}` (globalmente único → sem colisão na migração); helpers `Metadata.extract_topic/2`
  e `insert_topic/2`. Testado: migração sem perda + ranges/segments acompanham o topic.
  - ⏸️ **Desvio deliberado (não planejado): sharding de range/segment por range id (cross-vnode).** O
    NorthGuard sharda o *metadado* por range id; o malachi co-localiza o metadado de um topic num vnode
    (route por nome). **Decisão de não fazer** (avaliado): (1) os **dados já são shardados cross-node** —
    cada segment tem réplicas colocadas por HRW em nós distintos, então um topic movimentado já espalha
    dados por ranges→segments→nós; só a *granularidade de gestão do metadado* difere. (2) O sharding **por
    topic já espalha a carga de metadado** pelo cluster; o range-sharding só escalaria a taxa de *mutação
    estrutural* (splits/segments/s) de **um único** topic além de um grupo Raft — barra altíssima (mutações
    de metadado não são por-record). (3) O custo é reintroduzir **transações cross-Raft** para operações de
    topic (seal/delete tocam ranges em N vnodes), alocação de range id distribuída, `create`/`split`
    cross-vnode, e reads **scatter-gather** (desfazendo o índice O(1) do V-idx) — exatamente a complexidade
    que a co-localização evita. Pior custo/benefício do roadmap; **reavaliar só se surgir um gargalo
    concreto de metadado de um único topic**.

**Bridge control plane → data plane (1a.5):**
- ✅ `Malachi.Broker` — compõe o control plane (`Metadata`, fonte da verdade da estrutura) com
  o data plane (um `Log` por range, indexado por range id). `produce` roteia por chave usando os
  ranges ativos do `Metadata` + `Keyspace`; `split_range`/`merge_ranges` passam pelo `Metadata`
  (estrutura) e selam/flusham os logs afetados. As decisões lógicas vivem só no control plane.
- ✅ Leitura **cross-epoch** migrada para o `Broker` (`read_history`/`stream_history`, linhagem via
  `Metadata.parents`) + `Broker.pending?`. Validação de **nome de topic** no `Metadata.create_topic`
  (allowlist; rejeita `..`/`/`) — fecha o path-traversal no data plane, no control plane.
- ✅ `Malachi.BrokerServer` — GenServer sobre o `Broker`: flush por **tempo** (~10ms) + acesso
  **serializado** (concorrência), flush no shutdown. Paridade com o antigo `TopicServer`.
- ✅ **Duplicação removida:** `Topic`/`TopicServer`/`Range` **deletados** (sua orquestração de
  split/merge/coverage/lineage duplicava o `Metadata`). O caminho único agora é
  control plane (`Metadata`/`DSRSM`) + `Broker`/`BrokerServer`. **Bridge concluído.**

**Hardening do DS-RSM (property-based, substituto da simulação determinística):**
- ✅ Property tests model-based (`stream_data`) para `Metadata` e `DSRSM`: sequências aleatórias
  de create/split/merge/register/seal/delete + **split de vnode**, sempre escolhendo alvos
  válidos do estado atual. Invariantes verificadas: cobertura do keyspace por topic ativo,
  integridade referencial (range→topic, segment→range, id `{topic, seq}` bem-formado), **nenhum
  topic órfão** (sempre vive no vnode que o roteia, mesmo após split de vnode) e **determinismo**
  (mesma sequência → mesmo estado).

**Fase 1b — Replicação e membership:**
- ✅ **`ra` integrado** (Raft real): `Malachi.Cluster.MetadataMachine` é uma `:ra_machine` cujo
  `apply/3` **delega ao `Metadata.apply/2`** — a lógica de negócio não muda, só ganha replicação.
  `Malachi.Cluster.MetadataServer` é o wrapper fino (start de cluster single-node, `command` via
  log Raft, `query` linearizável, restart). Um cluster ra = um vnode; liderança = coordinator.
  Testado: comandos replicados, query consistente, erro de comando propagado, **estado sobrevive
  a restart** (log durável). Determinismo do `Metadata` (property tests) é o que torna isso seguro.
- ✅ **`Malachi.Cluster.ReplicatedDSRSM`** — o DS-RSM sobre Raft: HashRing + **um cluster `ra` por
  vnode**. `command`/`query` roteiam por nome do topic ao cluster do vnode dono (sharding +
  replicação por shard). Contraparte de produção do `DSRSM` puro (que os property tests exercitam).
  Testado: roteamento ao cluster certo, query consistente, **sharding entre 2 clusters Raft reais**
  (cada topic só existe no cluster que o roteia), erro de comando propagado, ring vazio → `:no_vnode`.
  Escopo: **vnodes estáticos** (split-sobre-ra adiado), single-node (multi-node depende de membership).
  - 🚧 **Split de vnode sobre `ra` (épico — migrar metadado entre grupos Raft, o que o NorthGuard faz:**
    *"break the state in half and basically spawn another raft group"*, transcrição do meetup). A lógica pura
    single-process já existe (`DSRSM.split_vnode` migra topics deslocados via `extract_topic`/`insert_topic`);
    falta a orquestração sobre os grupos `ra` reais. **Decisão de arquitetura do ring (corrigida por fidelidade):**
    o ring (topologia) é **estado global mínimo disseminado por gossip (SWIM)** — o que o NorthGuard faz
    (transcrição: *"we also use this dissemination for spreading some minimal global like cluster metadata"*;
    *"very minimal global states"*), **não** um cluster `ra` de topologia (mais CP, mas menos fiel). Reusa o
    gossip de atributos CRDT que o SWIM do malachi já tem. Fatiamento: **VS-1** primitivas puras + comandos ·
    **VS-2b** ring versionado disseminado (o ring é pré-requisito: sem propagação, um split fica inconsistente
    entre nós) · **VS-2a** orquestração do split sobre `ra` (subir o novo grupo, migrar pelo log, anexar a
    versão de ring) · **VS-2c** consistência (janela de cutover) · **VS-3** teste multinode.
    - ✅ **VS-1 — migração completa (com offsets) + comandos determinísticos.** Achado que a fatia corrigiu: o
      `Metadata.extract_topic` carregava topic+ranges+segments mas **não** os **committed offsets** (keyed por
      `{group, topic}`) → num split, um consumer group **perdia a posição** e reprocessava tudo (at-least-once,
      sem perda de dado, mas regressão séria). Agora `extract_topic`/`insert_topic` **carregam os offsets** (no
      export, keyed por group; o topic é implícito) — o binding de policy já viajava no struct do topic. E,
      pro split ra-backed dirigir a migração pelo **log Raft** de cada vnode, novos comandos determinísticos
      `:extract_topic` (reply = export) / `:insert_topic` no `Metadata.apply/2` (com catch-all defensivo
      intacto). Pura, in-VM, zero rede. Testado: extract carrega offsets por-group e deixa co-located intacto;
      round-trip preserva a posição; os comandos relocam via log; extract de topic ausente = no-op nil; +
      cobertura no `DSRSM` (split preserva offsets end-to-end). Suíte 786 testes 0 falhas (+5); credo/dialyzer/
      format limpos.
    - ✅ **VS-2b-1 — ring versionado disseminável (núcleo puro CRDT).** A fundação da propagação: novo
      `Malachi.Cluster.RingTopology` — a topologia de roteamento (`HashRing` + `%{vnode => [nodes]}`) tagueada
      com uma `version` monotônica. **Por que gossip e não um cluster `ra`:** o usuário perguntou "B não é mais
      fiel?" e a transcrição confirmou (linha 537: dissemina metadado global mínimo pelo SWIM; linha 610: estado
      global mínimo) — eu havia recomendado o cluster `ra` por instinto de CP, **corrigido** por fidelidade.
      `merge/2` é um **join CRDT**: maior `version` vence (last-version-wins), com tiebreak determinístico por
      ordem-total da serialização no raro clash de mesma versão (single-writer: só o líder `advance`) → merge
      **comutativo/associativo/idempotente**, converge em qualquer ordem de gossip. Puro, zero rede. Testado:
      `new`/`advance` (versão monotônica); merge mantém a maior versão, idempotente, comutativo (incl. no clash),
      associativo, converge à última versão em qualquer ordem — 6. Suíte 792 testes 0 falhas (+6); credo/dialyzer/
      format limpos.
      - ✅ **VS-2b-2a — disseminação do `RingTopology` pela gossip do SWIM.** O `MembershipServer` passa a
        **carregar a topologia junto** de cada mensagem de gossip (o mesmo caminho de disseminação que o
        NorthGuard usa pro estado global mínimo). Design de **churn mínimo**: um `gossip_payload/1` (view +
        topologia) substitui o builder de payload em todos os 8 sends, e o `merge_updates` passa a aceitar o
        payload `{updates, topology}` — com **cláusula defensiva** pra um peer que mande só a lista (novo-
        recebe-antigo, ex.: injeção de teste / gossip pré-topologia), e `merge_topology/2` nil-tolerante que
        faz o join CRDT do VS-2b-1. **Limitação anotada:** um nó **antigo** recebendo o payload novo quebraria,
        então um rolling upgrade completo do protocolo SWIM não é objetivo agora (deploy homogêneo). Nova API
        `set_topology/2` (adota a maior versão localmente; gossip leva adiante) / `topology/1`. Um handler de
        teste que inspecionava o payload cru foi ajustado pro novo shape. Testado in-VM: `set_topology` num nó
        **propaga** por gossip; **maior versão vence** em todos os nós, seja quem setou (last-version-wins); +
        o HA multinode do membership **verde** (gossip real com o novo payload não regride). Suíte 794 testes 0
        falhas (+2); credo/dialyzer/format limpos.
        - ✅ **VS-2b-2b — adoção local do ring quando a versão avança.** Fecha a propagação: o `MembershipServer`
          ganha um seam **`on_topology`** (padrão dos outros seams, ex.: `ranges_fun`) que dispara **só num
          avanço de versão** (`topology_advanced?`: `nil→qualquer`, ou `new > old`) — não em versão igual/menor —
          com o novo `RingTopology`, tanto via `set_topology` quanto via gossip. A app fia `on_topology`
          (`adopt_ring_topology/1`) pra **aplicar o ring novo ao `CoordinatorRouter`**: deriva o `servers`
          (`%{vnode => {vnode, nó}}`, qualquer membro; o router resolve o líder vivo) das `placements` e chama
          `put_topology`. O hook roda **inline** no server (deve ser leve e não levantar — a app respeita).
          **Escopo:** só o roteamento de consumer-group (`CoordinatorRouter`); a adoção do roteamento de
          **metadado** (`ReplicatedDSRSM`, acoplado ao runtime do broker) é dirigida pela própria orquestração
          do split (VS-2a), não por gossip. Single-node inalterado (o `MembershipServer` só sobe no modo
          clusterizado). Testado in-VM: o hook dispara no avanço (v1, depois v2), **não** dispara em versão
          igual/menor (`refute_receive`), e dispara num nó que aprende versão maior **por gossip**; boot
          single-node ok; HA multinode do membership verde. Suíte 796 testes 0 falhas (+2); credo/dialyzer/
          format limpos. **VS-2b (propagação do ring por gossip) concluído.**
      - ✅ **VS-2a — orquestração do split sobre `ra` (copy-first).** `ReplicatedDSRSM.split_vnode/4`: sobe o
        grupo `ra` do vnode novo, descobre os topics **deslocados** (os que passam a rotear pro novo sob o ring
        novo) e os **migra** entre os grupos `ra` — o que o NorthGuard faz (*spawn a new raft group + break off
        that half of the state*). Decisão **copy-first** (escolhida pra segurança): por topic, `:insert_topic` no
        novo → `:extract_topic` na origem, então **nenhuma falha isolada perde um topic** (crash pós-insert deixa
        um duplicado inócuo que o ring novo ignora). Snapshot linearizável da origem (`&Function.identity/1`, não
        closure, pra rodar no líder) → computa deslocados + exports **localmente** (puro) → aplica os comandos.
        Suporte pro copy-first: nova `Metadata.export_topic/2` (read-only) e o `extract_topic` **refatorado pra
        reusá-la** (DRY). Devolve o estado crescido só no sucesso total; falha de migração → `{:error, {:migrate,
        topic, ...}}` (split parcial fica pra reconciliar). **Escopo:** o mecanismo; **fora:** fencing de escrita
        concorrente no topic em migração (VS-2c) e a publicação do ring via `set_topology`/fiação no coordenador
        (integração). Testado `:multinode` (grupos `ra` locais, 3x estável): split de um vnode com topic + offsets
        → "orders" passa a rotear pro vnode novo, topic **e committed offsets** preservados lidos do grupo `ra`
        novo, e a origem **não** tem mais o topic (extract copy-first) + unit de `export_topic` (read-only, nil em
        ausente). Suíte 796 testes 0 falhas (+1 default, +1 multinode); credo/dialyzer/format limpos.
      - ✅ **VS-2c-1a — fence de migração (núcleo, seal-first).** O problema que fecha: durante a migração, uma
        escrita concorrente no topic (roteada pra origem, ring ainda velho) **depois** do snapshot seria perdida
        (o extract remove o estado atual, mas o insert usou o snapshot). Fix fiel ao NorthGuard (transcrição:
        *"we first seal R1 to create R2 and R3"* — selar dá as garantias de ordem): **fencer** o topic. Novo
        estado `migrating` (um set `%{name => true}` — map, não `MapSet`, pra evitar o atrito de tipo opaco do
        dialyzer com campo de struct) + comandos `:begin_migration`/`:end_migration`. O `apply/2` público virou um
        **guard central**: renomeei as 15 clauses pra `do_apply/2` e a `apply/2` agora, antes de despachar,
        resolve o **topic-alvo** do comando (`command_topic/2` — direto pra topic-scoped, via `range`/`segment →
        topic` pros de range/segment) e **rejeita `{:error, :migrating}`** se ele está fencido. Comandos de leitura
        e os de migração (create/define_policy/begin/end/extract/insert) **nunca** são fencidos. O `extract_topic`
        limpa o fence (o topic sumiu). Pura, determinística. Testado: `begin_migration` rejeita mutantes (incl. um
        comando de range que resolve pro topic) deixando o estado intacto; leituras e extract/insert passam;
        `end_migration` levanta; fence num topic não afeta co-located; begin em topic ausente erra — 5. Suíte 802
        testes 0 falhas (+5); credo/dialyzer/format limpos.
      - ✅ **VS-2c-1b — o `split_vnode` fencia a origem antes do snapshot.** Usa o fence do VS-2c-1a na
        orquestração: o `migrate_from` agora (1) lê a origem pra achar os deslocados, (2) **`:begin_migration`
        em cada um** (`fence_topics`), (3) **re-lê** a origem já estável (o re-read captura qualquer escrita que
        pegou a janela antes do fence), (4) migra copy-first do snapshot fencido — o `:extract_topic` **levanta**
        o fence (o topic sumiu). Assim, nenhuma escrita concorrente corre a cópia: fecha o gap que o VS-2a
        documentava. Numa falha parcial, os fences restantes **ficam de pé** (escritas bloqueadas nesses topics)
        pra reconciliação — fail-safe. Ressalva anotada: um topic **criado** durante o split que roteie pro novo
        não é pego (create não é fencido); o caller quiesce. Testado (`:multinode`, 2x estável): o split feliz
        segue verde (fence aplicado→levantado), e — novo — o topic migrado é **gravável no vnode novo** (o fence
        não vazou pro destino; um topic fencido responderia `{:error, :migrating}`). Suíte 802 testes 0 falhas;
        credo/dialyzer/format limpos.
      - ✅ **Int-1a — `BrokerServer.adopt_topology/2` (adoção pura do roteamento de metadado).** O desafio da
        integração: o roteamento de metadado do broker (o cache `dsrsm` com o ring + o `command_fun` sobre o
        `replicated`) é **capturado no boot** com o ring fixo, então um split runtime não é adotado. `adopt_topology`
        é a **função pura** (sub-fatia pure-first) que, dado o `Broker` + uma `RingTopology`, reconstrói o
        roteamento: o cache adota o **ring novo** (vnodes existentes mantêm o `Metadata` cacheado, um vnode novo
        começa **vazio** até o próximo refresh do `ra`) e o `command_fun` é reconstruído sobre o `%{vnode => server_id}`
        derivado das `placements` (skip de placement vazio, como o `adopt_ring_topology`). Pura: o catch-up do
        metadado do vnode novo é o side effect separado (refresh). Testado in-VM: adota o ring novo (v0+v1), v0
        mantém o topic cacheado, v1 vazio, `command_fun` reconstruído (arity 3). Suíte 803 testes 0 falhas (+1);
        credo/dialyzer/format limpos.
      - ✅ **Int-1b — o `BrokerServer` adota a topologia por gossip (async).** Fecha a adoção runtime do
        roteamento de metadado: um `handle_cast({:adopt_topology, topology})` reconstrói o roteamento
        (`adopt_topology/2` do Int-1a) **e** — a parte que faltava pra correção — o `metadata_refresh` e o
        `bootstrap.replicated` sobre o `replicated` novo, senão o `reconcile_metadata` periódico (que faz
        `snapshot(replicated_velho)` + `put_cache`, substituindo o cache inteiro, ring incluso) **reverteria** o
        ring adotado. O hook `on_topology` (VS-2b-2b, `adopt_ring_topology`) agora, além de atualizar o
        `CoordinatorRouter` inline, faz **`GenServer.cast`** ao `Malachi.LogBroker` — **async** (não bloqueia o
        membership server, que roda o hook inline) e **no-op se o broker não existe** (`cast` a nome ausente é
        `:ok`, verificado; single-node não roda o broker shardado). DRY: a derivação de `servers` virou
        `RingTopology.servers/1` (usada no broker e no router). Testado in-VM: o `handle_cast` adota o ring novo no
        cache **e** aponta o `bootstrap.replicated`/refresh pro ring novo (reconcile não reverte); `servers/1` pura
        (skip de placement vazio). Suíte 805 testes 0 falhas (+2); credo/dialyzer/format limpos. **Int-1 (broker
        adota em runtime) concluído. Próximo: Int-3 (coordenador de split sob lease: `split_vnode` +
        `set_topology` → tudo propaga e adota por gossip) + um teste `:multinode` de split de ponta a ponta.**
- ✅ **`Malachi.Cluster.Placement`** — política **pura** de placement + self-healing de réplicas
  de segment (a camada de *decisão* do data plane; o `Metadata` já guarda o *estado* dos segments).
  Usa **rendezvous (HRW) hashing**: `place/3` escolhe o replica set (determinístico → seguro p/
  Raft; mínimo reshuffle), `under_replicated/3` detecta segments com réplica morta (alvo clampado
  ao nº de brokers vivos), `heal/3` emite comandos `:set_segment_replicas` que restauram a
  replicação. Cobre segments **sealed** também (durabilidade). `available_brokers` é parâmetro
  abstrato — fixa o contrato que a futura membership servirá. Property tests: tamanho/distinção do
  replica set, determinismo independente de ordem, **retenção sob remoção de broker (min-reshuffle)**
  e **fixpoint do `heal` em uma passada**.
- ✅ **Ciclo de vida do segment no `Broker`** — o data plane agora *cria* segments. A cada range
  ativo o `Broker` mantém um segment aberto (span lógico de offsets sobre o `Log` único do range,
  A1): no 1º produce registra o segment escolhendo o `replica_set` via `Placement.place` sobre
  `:brokers`/`:replication_factor` (defaults `[node()]`/`1`); contabiliza os bytes apendados
  (`Record.encoded_size/1`, casado byte-a-byte com `encode/1`) e **sela + rola** ao cruzar
  `:segment_max_bytes` (threshold *soft*, checado no limite do batch). `split`/`merge` selam o
  segment ativo do range afetado. `segment_id = {range_id, seq}` (globalmente único; seq por-range
  persiste entre selas → id nunca reusado). API de `produce`/`read` inalterada (segments são
  bookkeeping aditivo). Testado: registro no 1º produce, replica set via HRW, rollover por bytes
  (1 record/segment e overshoot soft), sela em split/merge, validação de policy.
  - ⏳ Próximo: fiar `heal` (re-replicação) num gatilho de mudança de membership; rollover por
    tempo/contagem além de bytes.
- ✅ **`Malachi.Cluster.ReplicaTracker`** — núcleo **puro** de commit por quórum da replicação de
  **um segment** (a lógica determinística do mecanismo, sem processos/rede). `replica_set` ordenado
  (1º = primário); `ack/3` registra o offset durável de cada réplica (monotônico, ignora regressão);
  `commit_offset/1` = maior offset presente em **maioria** (⌊N/2⌋+1) das réplicas, ou `:none`;
  `committed?/2`. Tolera ⌊(N-1)/2⌋ falhas sem esperar a réplica mais lenta. Contraparte (para *dados*
  de segment) do `Metadata` determinístico (para *metadata*). Property tests: commit = maior offset
  em quórum, **monotonicidade do commit**, `committed?` sse quórum o tem. Escopo: replica set fixo
  (failover de primário/mudança de set depois).
- ✅ **`Malachi.Cluster.ReplicationServer`** — o **transporte** da replicação: um `GenServer` por
  broker que envia o stream do segment ativo do **primário** aos **seguidores** e dá ack quando um
  **quórum** armazenou de forma durável. Broker = referência do processo (nome local nos testes;
  `{nome, node}` entre nós — `GenServer.call` aceita ambos, então o mesmo caminho roda in-process e
  multi-node). `replicate/4` no primário apenda+fsync local, faz fan-out concorrente aos seguidores,
  alimenta o `ReplicaTracker` e retorna `{:ok, last}` quando o quórum tem o batch (tolera ⌊(N-1)/2⌋
  seguidores lentos/caídos) ou `{:error, :no_quorum}`. Primário e seguidores fazem `fsync` antes de
  contar para o quórum → "committed" = durável na maioria. Cada segment abre seu `Log` em
  `base_offset = start_offset` (carregado no `replicate/5`), então os offsets dos segments de um
  range são **contíguos** (não reiniciam em 0 por segment). Testado in-process: replica a todos +
  commit, tolerância a 1 seguidor caído, `:no_quorum` sem maioria, single-replica, offsets contíguos
  entre batches, **base não-zero** (`:out_of_range` abaixo da base), `:not_primary`, batch vazio,
  replica set vazio (sem crash), set duplicado (sem deadlock). Escopo: caminho feliz do segment ativo.
- ✅ **`ReplicationServer` fiado no `Broker`/`BrokerServer` (A2+A3)** — o `Broker` virou
  **roteador puro** (control plane): perdeu `logs`/`directory`, mantém só `Metadata` + ciclo de
  vida do segment + contador de offset por range. O storage é 100% do `ReplicationServer`. A
  ligação usa **funções de efeito injetadas**: `produce` recebe `replicate_fun`, `read`/
  `stream_history` recebem `read_fun` — toda a orquestração (rota, offset→segment, commit, paginação
  e filtro do cross-epoch) fica num só módulo, testado com **fakes in-memory**
  (`Malachi.Test.FakeSegmentStore`). O `BrokerServer` injeta `&ReplicationServer.replicate/5` e
  `&ReplicationServer.read/4`, inicia um `ReplicationServer` local (ref = pid) e abre o `Broker` com
  `brokers: [esse_ref]`. Como a escrita é fsync-por-quórum no retorno, o **flush por tempo (10ms)
  saiu** e `sync/1` virou no-op. `produce` retorna `{broker, {:ok, placements} | {:error, reason}}`
  (commit por grupo; valor imutável dá transação grátis). Sem duplicação de storage. Suíte completa
  verde (831 testes).
- ✅ **`Malachi.Cluster.Catchup` (primitiva de catch-up/backfill)** — copia os records de um
  segment de uma réplica **fonte** para uma **alvo** no intervalo `[from, to)`. Serve aos dois
  casos: seguidor atrasado fechando o gap do segment ativo, e réplica nova (do `Placement.heal`)
  fazendo **backfill** de um segment selado inteiro. Roda por **orquestração externa** (no processo
  do chamador, via `ReplicationServer.read/4` na fonte + `follow/4` no alvo) — nada aninhado num
  `handle_call` que está sendo aguardado, então não há o deadlock primário↔seguidor. Expostos no
  `ReplicationServer`: `follow/4` (append de réplica) e `end_offset/2` (até onde o alvo está, ou
  `:empty`). Se a fonte estiver ela mesma atrás, para no que tem e devolve o offset alcançado.
  Testado in-process: backfill de réplica fresca, catch-up só do gap, base não-zero preservada,
  fonte incompleta, no-op quando já alcançado, `:empty`. Escopo: a primitiva (dirigida pelo
  chamador).
- ✅ **`Malachi.Cluster.SelfHealing` (heal → backfill de selados)** — fecha o loop de self-healing
  para segments **selados**: `Placement` decide, `Catchup` executa. `heal_sealed/4` (metadata +
  brokers vivos + rf) acha os selados sub-replicados, escolhe o replica set curado via
  `Placement.place`, faz **backfill** de cada réplica nova a partir de uma réplica viva
  (`Catchup.run`), e devolve os comandos `:set_segment_replicas` (que tiveram backfill ok) para o
  control plane aplicar, além dos segments que não deu para curar (ex.: `:no_live_source`). Só
  selados (range `[start, start+length)` fixo → backfill bem-definido); o segment ativo se recupera
  pelo gatilho no write-path (fatia (a), adiada). Brokers vivos vêm como parâmetro (membership
  depois). Testado in-process: backfill da réplica nova + comando + loop fecha (re-heal vazio),
  todas as réplicas mortas → `:no_live_source`, nada a fazer quando ok, ativo ignorado.
- ✅ **Catch-up automático no write-path (segment ativo)** — fecha a metade que faltava da
  recuperação (a de selados é o `SelfHealing`). Quando o fan-out do primário chega num seguidor com
  `next_offset < expected_first` (ficou para trás), o `ReplicationServer` **dispara um processo
  monitorado em background** que roda o `Catchup` puxando do primário (`from = seu próprio fim`,
  `to = fim do primário`), responde `:out_of_sync` (a escrita commita via as réplicas em dia) e o
  seguidor **reentra no quórum num batch posterior**. Um conjunto `catching_up` deduplica catch-ups
  por segment; o `:DOWN` do monitor limpa o flag em sucesso/falha; a checagem de offset do `follow`
  garante segurança sob corrida (um append concorrente faz o catch-up abortar e o próximo gap
  re-disparar — converge, sem duplicar/corromper). A mensagem de `follow` carrega o ref do primário
  como fonte; os `follow` do `Catchup` passam `nil` (não re-disparam). Testado in-process:
  seguidor perde um batch → é caçado de volta ao log completo → reentra no quórum.
- ✅ **Backfill de réplica nova em segment ativo (missed-start)** — extensão mínima do gatilho: a
  mensagem de `follow` passa a carregar o `base` do segment e o log fresco abre **no `base`** (não
  no offset do batch). Assim uma réplica **recém-adicionada** a um segment ativo vê o gap do começo
  (`next = base < expected`), o gatilho existente puxa `[base, cabeça)` e ela **converge na cabeça
  móvel** via os re-disparos missed-middle, entrando no write-path ao alcançar (estilo ISR: não conta
  no quórum até sincronizar). Zero código de gatilho novo. Limitação conhecida: sob escrita sustentada
  muito rápida pode não alcançar (sem throttling — refinamento futuro). Testado in-process: réplica
  nova num segment ativo backfilla do começo e passa a seguir ao vivo.
  - ⏳ Próximo: failover de primário; multi-node (membership/SWIM).
- ✅ **`Malachi.Cluster.Membership` (máquina de estado SWIM pura)** — a view determinística de
  membership: `alive`/`suspect`/`dead` por membro + **incarnation**, sem processos/timers/rede. A
  regra única é um *join* na ordem lexicográfica `{incarnation, rank}` (`alive < suspect < dead`),
  que dá a **precedência SWIM** (incarnation maior vence; empate → suspect>alive, dead>ambos; igual
  → idempotente) e garante **convergência** do merge de gossip em qualquer ordem (CRDT-like).
  Exceção: update sobre **self** suspeito/morto é **refutado** subindo a própria incarnation e
  re-anunciando alive (efeito `{:refute, …}` para disseminar). `apply_update/2`, `merge/2`,
  `suspect/2`, `confirm/2`, `alive_members/1`. Property tests: **convergência independente de ordem**
  e **monotonicidade de incarnation**; + unidades de precedência e self-refute.
- ✅ **`Malachi.Cluster.MembershipServer` (servidor SWIM: detector + gossip)** — `GenServer` por
  broker que roda a `Membership` ao vivo. A cada *protocol period* faz **ping** num peer vivo
  aleatório; sem **ack** no `ack_timeout` → `suspect`; sem refute no `suspicion_timeout` → `dead`.
  Cada ping/ack faz **piggyback** da view (lista de updates) → anti-entropy → as views **convergem**;
  um nó falsamente suspeito descobre pelo ack ao seu próprio ping e refuta (sobe incarnation). Refs
  location-transparent (testável in-process; multi-node sem mudança); envios fire-and-forget (peer
  morto = sem ack = detecção). Peers semente via `:peers`; timers configuráveis. Testado in-process:
  **gossip espalha** conhecimento parcial até todos conhecerem todos, **nó parado é detectado e vira
  `dead`** no cluster, e **refute** de suspeita falsa sobre si.
- ✅ **Ping indireto** — quando o ack direto não chega, o nó pede a `indirect_fanout` peers (default 3)
  que **pinguem o alvo por ele** (`ping_req` → relay proba o alvo → relaia o ack ao requester); só
  suspeita se nem o direto nem o indireto responderem no `indirect_timeout`. Reduz falsos positivos sob
  perda transitória da rota direta. Testado: a **cadeia de relay** entrega o ack do alvo ao requester
  (determinístico); o teste de nó morto agora exercita o caminho completo (direto→indireto→suspect→dead).
- ✅ **Join formal** — ao iniciar, o nó manda `{:join}` a cada seed (`:peers`); o seed **registra o
  joiner vivo** e responde com sua **view completa**, então o joiner conhece o cluster de uma vez (em
  vez de só convergir por gossip). Best-effort (gossip é a rede de segurança). Testado: handshake
  (seed responde `join_ok` com a view e passa a conhecer o joiner) e **nó novo aprende o cluster
  inteiro via um único seed**. Ressalva: rejoin-após-morte (reinício com incarnation 0 < `dead@n`)
  fica para depois. **SWIM fechado:** ping direto+indireto + suspicion + gossip + join.
- ✅ **`Malachi.Cluster.HealCoordinator` (self-healing reativo)** — `GenServer` periódico que fecha
  o loop **broker morre → detectado → curado**: a cada intervalo roda `SelfHealing.heal_sealed` com
  o conjunto **vivo** e aplica os comandos. Desacoplado por **seams injetados** (`live_brokers`,
  `metadata_source`, `apply_command`, `rf`, `interval`) — testável in-process e fiável depois ao
  `MembershipServer` (live) e ao control plane (apply) sem mudança. `heal_now/1` roda uma passada
  síncrona (testes/trigger manual). Testado in-process: cura sob réplica perdida (backfill +
  comando aplicado + loop fecha), `:no_live_source`, e **cura automática no timer**.
- ✅ **Fiação fina 1a — membership → self-healing reativo (controle único + N data-brokers)** — o
  loop reativo agora roda ponta a ponta na topologia **1 nó de controle + N data-brokers**: o
  `BrokerServer` aceita `:brokers` externos (refs de `ReplicationServer`) e é a autoridade do
  metadata; ganhou `metadata/1` e `apply_heal/2` (que aplica `set_segment_replicas` via novo
  `Broker.apply_heal/2`). O `HealCoordinator` é fiado com `metadata_source`/`apply` ← `BrokerServer`
  e `live_brokers` ← **bridge** `MembershipServer.alive_members |> mapa(member-id → broker-ref)`.
  Testado: **data-broker morre → membership detecta → segments selados re-replicados** ao conjunto
  vivo (sem sub-replicação restante, dados conferidos nas novas réplicas), e a bridge derruba a
  broker-ref do nó morto.
- ✅ **Placement dinâmico** — o `BrokerServer` agora **refresca** periodicamente os brokers de
  placement do `Broker` a partir de `:live_brokers` (`Broker.set_brokers/2`), então um **novo segment
  nasce no conjunto vivo** em vez de nos configurados (fallback: mantém o último não-vazio; sem custo
  no hot path). Completa a simetria do reativo (cura **e** criação reagem ao vivo). Robustez junto:
  `ReplicationServer.replicate/5` agora **não derruba** o chamador se o primário estiver morto
  (retorna `{:error, :unreachable}` → `produce` propaga erro), e um `produce` falho **descarta** o
  segment recém-aberto (rollback grátis via valor imutável — sem segment fantasma; o retry re-placeia
  nos vivos). Testado: após um data-broker morrer e sair do conjunto vivo, um segment recém-produzido
  **exclui o morto** do `replica_set`.
- ✅ **Failover de primário** — `Malachi.Cluster.Failover.plan/2` (puro): para cada segment **ativo**
  cujo primário (`hd(replica_set)`) está morto, emite `set_segment_replicas` **reordenado** com uma
  réplica viva no topo (a morta fica como seguidor que não dá ack; o `heal` a troca após o seal —
  sem mover dado). O `Broker.apply_heal` foi generalizado para atualizar também o **cache do segment
  ativo** (um só caminho de apply serve heal e failover), e o `HealCoordinator` virou um loop de
  **reconciliação** (heal selados + failover ativos a cada tick). Testado: `plan` puro (promove vivo,
  ignora selado, pula tudo-morto), `apply_heal` atualiza cache+metadata, e **integração**: primário
  de segment ativo morre → promovido → escrita continua no novo primário (ambos os records lidos).
- ✅ **1b — autoridade do metadata via `ra` (em fatias).**
  - ✅ **`Malachi.Cluster.ReplicatedMetadata`** (componente) — pareia um `MetadataServer` (cluster `ra`
    rodando `Metadata.apply`) com um **cache `Metadata` local**: `command/2` vai ao log Raft e, no
    commit, aplica o **mesmo** comando no cache (determinismo → cache == estado replicado), então as
    leituras são locais (sem round-trip Raft no hot path) com **read-your-writes**. `metadata/1` (leitura),
    `refresh/1` (re-lê o estado replicado p/ multi-writer/recovery), `start/1`, `delete/1`. Testado:
    comando commitado atualiza o cache, comando rejeitado deixa o cache intacto + erro da máquina,
    cache == estado replicado (refresh no-op p/ escritor único). Cluster `ra` single-node (durabilidade;
    HA multi-nó depois).
  - ✅ **`BrokerServer` fiado no metadata via `ra`** — o `Broker` ganhou um **`command_fun` injetado**
    `(metadata, command) -> {metadata, reply}` (default `&Metadata.apply/2` → comportamento in-memory
    intacto); **todas** as mutações (`create_topic`/`split`/`merge`/`register`/`seal`/
    `set_segment_replicas`) passam por ele, leituras seguem no `broker.metadata` (cache). Com a opção
    `:metadata_cluster`, o `BrokerServer` inicia um `MetadataServer`, **semeia** o cache do estado
    replicado, e injeta `command_fun = ReplicatedMetadata.apply_command(server_id, …)` — comando Raft +
    apply no cache **threadado pelo `broker.metadata`** (um produce pode abrir+selar segment com
    read-your-writes intra-operação). Testado (ra): mutações (topic + segment) ficam no **estado
    replicado** (query direto no `ra`) e o cache == replicado; comando rejeitado propaga o erro da
    máquina. Default (sem `:metadata_cluster`) segue in-memory.
  - ✅ **Hardening do crash-path do `register`** — `open_segment` agora retorna `{:ok, broker}` |
    `{:error, reason}`; uma falha do `register_segment` (ex.: timeout do `ra`) é propagada pelo
    `ensure_segment` → `produce` como `{:error, reason}` (rollback do open — sem segment fantasma),
    em vez de derrubar o `BrokerServer`. Testado: `command_fun` que falha o register → produce
    `{:error, :ra_down}`, nenhum segment registrado, offset não avançado.
  - ✅ **Cluster `ra` multi-nó (HA do control plane — último SPOF eliminado)** — `MetadataServer.start/2`
    forma o cluster `ra` em **vários nós** (`server_ids` entre nós); `BrokerServer` aceita
    `:metadata_nodes`. Com ≥3 membros o metadata é replicado e **sobrevive à queda de um nó de
    controle** (um seguidor é eleito líder). Testado de verdade (`test/.../metadata_ha_test.exs`, tag
    `:multinode`, opt-in via `mix test --include multinode`): 3 nós BEAM reais via `:peer`, comando
    replicado a todos, **kill abrupto do nó-líder**, e um comando seguinte ainda comita (novo líder) com
    o metadata anterior intacto — flake-checado 5×. (Excluído do `mix test` padrão por exigir
    epmd/distribuição.) **1b concluído.**

### Fase 3 — Produto: conectar os dois mundos + escala (novo norte)
Tornar o stack NorthGuard o broker **vivo** e escalável, melhor que o Kafka OSS.

- ✅ **B — Conectar TCP/cliente ↔ stack NorthGuard (log API com cursor opaco).** Hoje o `tcp_protocol`
  fala filas (`publish`/`subscribe`/`ack`/`channel_*`) sobre `Queue`/`Channel`; o stack NorthGuard fala
  log sobre `BrokerServer`. **Contrato de cliente = jeito NorthGuard, NÃO Kafka:** o cliente usa
  `topic` + **chave** (produce) + **cursor opaco** (consume) — **nunca** vê partition/offset (escondidos
  de propósito, para o sistema split/merge/restripe por baixo sem quebrar o cliente; é o diferencial vs
  Kafka). O cursor é um token que hoje codifica `%{range_id => offset}` por dentro, mas o cliente o trata
  como opaco. **Prioridade #1.** Fatias:
  - ✅ **1ª fatia — núcleo `Malachi.LogApi`:** `create_topic`/`produce` (por chave)/`fetch` (cursor
    opaco) sobre `BrokerServer`; decode do cursor **seguro** (`binary_to_term [:safe]` + validação de
    forma). Cliente nunca vê partition/offset. Testado isolado.
  - ✅ **1ª fatia — fiação:** `BrokerServer` (`Malachi.LogBroker`) subido na árvore de supervisão
    (`application.ex`, single-node/in-memory por default; data dir via `:log_data_dir`); ações
    `create_topic`/`produce`/`fetch` adicionadas ao `tcp_protocol` (aditivo — filas/canais continuam),
    com auth (`:produce`/`:consume`), `max` limitado, records→JSON **sem offset**. **Cliente alcança o
    stack NorthGuard de ponta a ponta** — testado e2e via TCP real (produce por chave → fetch por
    cursor opaco; permissões; re-fetch vazio).
  - ✅ **Consumer groups + posição commitada server-side (Opção A — offsets no Metadata RSM).** Um grupo
    consome com `fetch_group(topic, group)` (retoma da posição **duravelmente** commitada do grupo) e
    avança-a com `commit(topic, group, cursor)` (estilo at-least-once / commit manual do Kafka). O cursor
    segue **opaco** (o cliente nunca vê offset). Os offsets vivem no `Metadata` determinístico
    (`{:commit_offset, group, topic, offsets}` + query `committed_offsets/3`), então ganham durabilidade
    e HA **de graça** pelo caminho `command_fun`/`ra` existente. Exposto no `BrokerServer`/`LogApi` e no
    `tcp_protocol` (`fetch` aceita `"group"`; nova ação `commit`). Testado: unit do Metadata (last-commit-wins,
    grupos/topics independentes), round-trip `LogApi` (retoma após commit; re-leitura sem commit = at-least-once),
    e2e via TCP (produce → fetch por grupo → commit → fetch retoma vazio). Offsets commitados crescem o
    estado do Metadata (escala → futuro store log-based, estilo `__consumer_offsets`, alinhado ao roadmap D).
  - ✅ **Consumo split-aware (cursor cobrindo ranges que dividem).** Antes, o consumo lia só o offset
    linear de cada range **ativo**, então registros escritos **antes** de um split (que vivem nos segments
    do range-pai, agora selado e fora do conjunto ativo) eram **perdidos**. Agora `read_consume/5` no
    `Broker` faz leitura **cross-epoch ao vivo**: drena os ancestrais selados (filtrados à fatia de
    keyspace do range, via `parents` + `Keyspace`) e depois faz **tailing** do range ativo — sem marcar
    `:done` no self (diferente de `stream_history/5`, que é para história *bounded*), reusando
    `history_sources`/`filter_records` (DRY). A posição por range vira um `consume_cursor`
    (`:start | {source_index, source_offset}`), carregada opacamente no cursor do cliente e nos offsets
    commitados (tipo `Metadata.position`). `fetch`/`fetch_group` herdam o fix. **Split durante o consumo
    (decisão A):** os filhos não herdam a posição do pai → recomeçam do início e **reprocessam** sua fatia
    (at-least-once, zero perda; splits são raros/administrativos). Testado: `Broker` (entrega cross-epoch
    exata pós-split, tailing de novos registros, range inexistente) e integração `LogApi`/`BrokerServer`
    (fresh consumer vê todos os pré+pós-split; grupo commitado reprocessa após split).
  - ✅ **Payloads binários (base64 no protocolo JSON).** O storage e o `LogApi` sempre aceitaram bytes
    arbitrários (`build_record` é `is_binary(value)`); o gargalo era só a borda JSON: o `value` vinha do
    `Jason.decode` (sempre UTF-8) e, no fetch, um `value` não-UTF-8 **derrubava o `Jason.encode!`**.
    Agora **todo `value` no wire é base64** (escolha do esquema: uniforme): o `produce` decodifica
    (`Base.decode64`) cada `value` para bytes antes de chamar o `LogApi`, e o `record_to_json` codifica
    (`Base.encode64`) no fetch. base64 vive **só no `tcp_protocol`** (o `LogApi` segue binário-nativo);
    `key`/`headers` permanecem UTF-8. base64 inválido → `:invalid_base64`. **Breaking** (clientes JSON
    agora mandam/recebem `value` em base64). Testado e2e: round-trip de bytes não-UTF-8, base64 inválido
    rejeitado, e os testes existentes migrados para base64.
  - ✅ **Long-poll no `fetch`/`fetch_group` (event-driven, *waiters* no `BrokerServer`).** Antes, um
    consumidor *caught up* recebia `[]` na hora e tinha que re-pollar (busy-poll). Agora um `wait_ms`
    opcional (clampado a 30s no `tcp_protocol`) faz a chamada **bloquear** até um produce ao topic
    entregar dados ou o timeout expirar (`[]`). Mecanismo escolhido após **benchmark** (`bench/`): A —
    *waiters* dentro do `BrokerServer` (o GenServer que já serializa produce+fetch) guardam os fetches
    pendentes vazios (`{from, topic, positions, max}` + timer); o `produce` ao topic re-consome cada
    waiter e responde (`GenServer.reply`) os que têm dados, o timeout responde `[]`. Medições: A entrega
    ~35-47% mais rápido que pub/sub via Registry (B) em todas as escalas e é **self-contained** (sem
    estado global); o contra de A (produtor bloqueia no fan-out) só é material com milhares de
    consumidores no mesmo topic — fora do single-node atual; ao ir multi-nó, o fan-out migra para um
    mecanismo distribuído. A orquestração de leitura multi-range **migrou** do `LogApi` para o
    `BrokerServer` (`consume_ranges`): 1 call coesa em vez de N+1, e é o que o produce re-executa para
    acordar waiters. Testado: `BrokerServer` (acorda no produce, timeout vazio, acorda só o topic
    produzido), `LogApi` (bloqueia até produce; timeout), e2e TCP (wake por produtor concorrente; timeout).
  - ✅ **Rearquitetura da camada de cliente (protocolo binário + streaming), guiada por benchmark.** A
    funcionalidade de log sobre TCP estava completa, mas o **estilo** era JSON+base64 request/response —
    não o "sessionized streaming com windowing" do NorthGuard. Decidido **empiricamente** (`bench/`,
    1M msgs): (a) **protocolo binário** vs JSON+base64 — **-29% bytes on-wire, 8.9x menos CPU no encode,
    17.3x no decode** (`protocol_bench.exs`); (b) **entrega push** (subscribers no broker) 35-44% mais
    rápida que Registry pub/sub, **mas sem windowing a mailbox explode** (consumidor lento → pico 190.276)
    → **OOM**; com janela fica em 999 (`streaming_bench.exs`); (c) baseline do sistema 657k produce /
    1.2M consume rec/s, memória plana (`throughput_1m.exs`). Alvo NorthGuard-fiel = **push + windowing** +
    **protocolo binário**. Autorizado **remover o modelo de fila legado** (queues/channels) e **substituir**
    o JSON pelo binário. Frentes: **B1** protocolo binário → **B2** streaming push+windowing → **B3** remover
    filas legado. Cada uma fatiada (núcleo testável + fiação).
    - ✅ **B1a — codec binário (`Malachi.Wire`, núcleo puro).** Framing length-prefixed
      (`<<len::32, body>>`), envelope request `<<api_key::16, correlation_id::32, payload>>` / response
      `<<correlation_id::32, error_code::16, payload>>` (o `correlation_id` habilita **pipelining**), e os
      codecs das 4 operações de log (create_topic/produce/fetch/commit). Records **sem offset** no wire (o
      cliente nunca vê offset; o cursor opaco carrega a posição) — encoding próprio, distinto do
      `Record.encode/1` de disco. Cursor/key são byte-strings com flag de presença (`nil` ≠ vazio). Puro;
      a fiação no socket é B1b. Testado: round-trip de frame (+ `:incomplete` em prefixo parcial, dois
      frames num buffer), envelope, wire-record (nil-key vs vazio, bytes não-UTF-8, property de round-trip),
      e as 4 operações.
    - ✅ **B3a — remover o *protocolo* de fila (antes de B1b).** Ordem invertida: mudar o loop de conexão
      para frames binários torna as 14 ações JSON de fila/canal inacessíveis e quebraria seus testes;
      então o legado de *protocolo* de fila sai primeiro, deixando o `tcp_protocol` só com log — aí B1b
      converte log JSON→binário limpo. Removidas as 14 `handle_action` de fila/canal e seus helpers
      (`publish`/`enqueue`/`build_queue_options`/backpressure/…) do `tcp_protocol` (1130→~200 linhas, só
      create_topic/produce/fetch/commit + auth/permissão compartilhados); removido o modo `subscribed`/
      `receive_active_loop` do `tcp_acceptor` (era push de fila — B2 reintroduz um loop de *streaming de
      log* próprio). Os **módulos** `Queue`/`Channel`/etc. ficam (B3b os deleta + ajusta metrics/dashboard).
      Testes: `tcp_queue_management` (100% fila) deletado; os 7 de protocolo/segurança via socket
      (`tcp_protocol`, `channel_integration`, `comprehensive_security`, `protocol_fuzzing`, `rate_limiting`,
      `validation`, `penetration`) **pulados** (`@moduletag :skip`) para reescrever contra o binário em B1b
      (evita migrá-los para um log-JSON que some) — a infra (`Auth`/`RateLimiter`/`Validator`) segue coberta
      por seus unit tests, e o **log e2e** (`log_protocol_test`) continua verde. Suíte: 1087 testes, 0
      falhas, 94 skipped; credo + dialyzer limpos.
    - ✅ **B1b-i — fiação binária (e2e).** O `tcp_acceptor` lê frames `Wire` length-prefixed (o listen
      socket mudou de `packet: :line` para `packet: 0` — raw; o `Wire` faz o framing) num buffer que
      tolera frames divididos em vários `recv` (`decode_frame` → `:incomplete`); o **auth** também virou
      binário (novo `api_key` 0, req username/password → resp token). O `tcp_protocol.process_frame/4`
      decodifica o request **na borda com `try`** (frame malformado → um frame de erro, sem crashar),
      roteia por `api_key` e responde binário via `Wire.encode_ok`/`encode_error` (error_code 0 = ok, 1 =
      erro com a reason como string). `LogApi.produce_records/3` (novo) aceita `%Record{}` direto do wire,
      pulando o passo map→record do `produce/3` JSON; o `fetch_req` ganhou `group` (cursor tem
      precedência). O `TCPHelper` ganhou os helpers binários (`request`/`recv_frame`/`authenticate_wire`)
      e o `log_protocol_test` foi **reescrito** para o binário (o caso de base64 saiu — o valor é bytes
      nativos; o de cursor malformado virou bytes inválidos). Testado: log e2e binário (create/produce/
      fetch/grupos/commit/binário/long-poll/permissões) + `Wire` (auth/ok/error round-trip). Suíte: 1055
      testes, 0 falhas, 94 skipped; credo + dialyzer limpos.
    - ✅ **B1b-ii — cobertura de segurança do binário.** Os 6 testes de protocolo pulados no B3a eram
      ~100% fila/JSON (queue/channel/publish/subscribe como veículo, fuzzing de **JSON**, validação de
      `queue_name` — tudo obsoleto no binário sem filas), então em vez de adaptá-los 1:1 foram **deletados**
      (+ `channel_integration`) e substituídos por um `binary_protocol_security_test` coeso sobre o
      protocolo que **de fato** roda: auth (credenciais inválidas → frame de erro; primeiro frame não-auth
      → `auth_required`; primeiro frame malformado não crasha), permissões (produce→`:produce`,
      fetch→`:consume`), frames malformados autenticados (`api_key` desconhecido → erro e a conexão segue
      servindo; payload truncado → `malformed_request` com o correlation id preservado) e fuzzing
      (bytes aleatórios + length-prefix mentiroso → o servidor sobrevive, provado por uma conexão nova).
      Ajuste no acceptor: credenciais inválidas agora respondem um frame de erro (o auth JSON antigo só
      fechava). Os helpers JSON do `TCPHelper` (`send_line`/`recv_line`/`authenticate`) foram removidos (só
      binário resta); a infra (`Auth`/`RateLimiter`/`Validator`/`LockoutManager`) segue coberta por seus
      unit tests (`input_fuzzing`/`attack_simulation`/`security_performance_regression`, via o
      `SecurityHelper` puro). **Suíte: 969 testes, 0 falhas, 0 skipped; credo + dialyzer limpos. B1 completo.**
    - **B2 — streaming push+windowing (sessionized streaming NorthGuard).** O gap da tabela ("windowing
      por stream"). Decisões (alinhadas ao NorthGuard, revistas por dúvida do usuário): flow control por
      **ack explícito de crédito** (o que o benchmark validou; sem ele → OOM) e posição por **grupo
      durável** (o *consumer-group management* do NorthGuard, não cursor efêmero — reusa o `commit`
      existente). O ack faz **dupla função: devolve crédito da janela E commita a posição do grupo**
      (at-least-once).
      - ✅ **B2-a — subscribers no `BrokerServer` (núcleo).** Estado `subscribers: %{topic => [sub]}`
        (`sub = %{pid, ref, topic, group, positions, window, in_flight, max}`). `subscribe/5` registra
        (+`Process.monitor`), carrega a posição commitada do grupo (`committed_offsets`) e faz um push
        inicial; o `produce` chama `wake_subscribers/3` (irmão do `wake_waiters/3`) que empurra o que
        **couber na janela** (`min(max, window - in_flight)` via `consume_ranges`, enviando
        `{:log_records, topic, records, next_positions}`); `stream_ack/5` **commita** a posição
        (`commit_offset`, durável) e **devolve `count` crédito** liberando mais pushes; `:DOWN`/
        `unsubscribe/2` removem. Puro do lado do socket — o subscriber é um pid. Testado (in-process):
        backlog no subscribe + push no produce; a janela limita in-flight até o ack; o ack commita
        durável; nova subscrição do grupo retoma da posição commitada; subscriber morto é removido via
        `:DOWN`. Dialyzer/credo limpos; broker+log e2e verdes.
      - ✅ **B2-b — fiação binária do streaming.** `Wire` ganha `api_key` `subscribe` (5) e `stream_ack`
        (6) + codecs (`encode/decode_subscribe_req` = topic/group/window/max; `encode/decode_stream_ack_req`
        = topic/group/cursor/count); o **push reusa `encode_fetch_resp`** (records + cursor opaco). Decisões
        (defaults técnicos naturais): **um stream por conexão** (o `subscribe` toma a conexão), **push =
        response com o `correlation_id` do subscribe** (o cliente associa aquele corr_id ao stream, à la
        gRPC streaming), **ack fire-and-forget** (sem response — o "resultado" são mais pushes). O
        `TCPProtocol.process_frame` devolve `{:stream, corr}` no `subscribe` (após gate `:consume` +
        registro via `LogApi.subscribe`); o `tcp_acceptor` então troca a conexão para **active mode** e
        entra no `stream_loop` full-duplex: um `receive` trata os `{:log_records, ...}` do broker
        (encaminhados como frames de push — `LogApi.encode_cursor` positions→cursor, agora público) **e** os
        frames de ack do cliente (`process_stream_frame/3` → `LogApi.stream_ack`, decode cursor→positions).
        Sem frame de unsubscribe: a conexão fechada mata o processo → `:DOWN` no `BrokerServer` remove o
        subscriber (o cleanup do B2-a). Window/batch são limitados server-side (`stream_window`/`fetch_max`).
        E2e via TCP (`log_streaming_test`): backlog no subscribe + push num produce de outra conexão; a
        janela limita in-flight até o ack devolver crédito; `subscribe` sem `:consume` recebe erro (não vira
        stream). Dialyzer/credo limpos; 385 testes verdes.
    - ✅ **B3b — deletar o modelo de fila legado (sub-fatiado; ordem forçada pelas deps de compilação:
      callers antes de callees).** O modelo (queues/channels) está **morto no caminho vivo** — nada fora
      dos próprios módulos + periféricos (metrics/dashboard/backpressure/benchmark/application) chama
      `Queue`/`Channel`/`Consumer`/etc. Decisão do dashboard: **aparar para sistema-só** (1A), não remover
      (preserva o servidor HTTP + auth + painel de sistema/TLS, model-agnostic; painel NorthGuard de
      topics/streams fica para depois).
      - ✅ **B3b-i — aparar o dashboard.** `dashboard.ex` deixa de renderizar/buscar filas/canais:
        `serve_metrics`/`stream_metrics` emitem só `%{system: get_system_metrics()}` (sem enriquecimento
        via `ConnectionRegistry`); removidos os cards HTML de Queues/Channels, o JS de fila/canal
        (`renderQueues`/`renderChannels`/`renderConnectionList`/`changeQueuePage`/`escapeHtml`/`formatTime`),
        o CSS morto (queue-card/channel-card/pressure/utilization/connection/pagination) e o alias órfão
        `ConnectionRegistry`. As rotas (`/`, `/metrics`, `/stream`, `/rate_limits`, auth) ficam intactas.
        Desacopla o dashboard dos getters de fila do `Metrics` (pré-requisito de B3b-iii). Testes de
        dashboard (status/rota) verdes (30); credo/dialyzer limpos. `security_xss_test` ficou stale
        (referencia o `escapeHtml`/nomes de fila removidos; ainda passa, é tautológico) — limpeza à parte.
      - ✅ **B3b-ii — deletar o núcleo do modelo de fila.** Removidos os 6 módulos-núcleo
        (`queue`/`channel`/`consumer`/`ack_manager`/`partition_manager`/`queue_config`) + `benchmark.ex`
        (superado por `bench/*.exs`) + `backpressure.ex`, e as 7 entradas de supervisão do `application.ex`
        (`QueueRegistry`/`ChannelRegistry`/`Queue`/`ChannelSupervisor`/`PartitionManager`/`QueueConfig`/
        `AckManager`). No `metrics.ex`, os **getters** que dependiam desses módulos saíram já aqui (forçado
        pela compilação: `get_metrics`/`get_all_metrics`/`get_channel_metrics`/`get_all_channel_metrics` +
        privados órfãos `get_gauge`/`get_latency_stats`/`get_all_queues`/`get_all_channels`; `take_snapshot`
        agora captura só sistema) — os **contadores** puros de ETS (increment_*/record_latency/reset) ficam
        para B3b-iii. Testes: deletados os puros de fila (queue/channel/consumer/ack_manager/
        partition_manager/queue_config/integration/at_most_once/one_to_million/atom_exhaustion/
        overflow_integration/backpressure + helpers mass_spawn/test_helpers); adaptados os mistos
        (`application_test`/`malachimq_test` → apontam para o stack de log; `attack_simulation` → removidos
        os 2 testes de fila, mantidos os de segurança; `atom_safety` → mantidos só Validator+AtomMonitor;
        `metrics_test` → reduzido a system-metrics+history). Também removida a **duplicata stale**
        `test/application_test.exs` (colidia com `test/malachi/application_test.exs` no mesmo módulo
        `Malachi.ApplicationTest` — bug latente exposto pelo compilador paralelo). 783 testes, 0 falhas;
        credo/dialyzer limpos (−23 arquivos).
      - ✅ **B3b-iii — remover os contadores de fila/canal mortos do `metrics.ex`.** Deleção pura de código
        morto (a superfície de teste já ficou limpa no B3b-ii): saem `increment_enqueued`/`processed`/
        `errors`/`acked`/`nacked`/`retried`/`dead_lettered`/`rejected`/`dropped`, `increment_channel_*`,
        `set_blocked_producers_count`/`increment_total_producers_blocked`/`record_buffer_utilization`,
        `record_latency` e `reset_metrics`. Sobra o que é operacional/segurança (rate-limit, connection-limit,
        validation, auth/lockout, audit, dashboard-auth, TLS) + `get_system_metrics` + `get_history`;
        moduledoc atualizado. 783 testes verdes (1 flake **pré-existente e não relacionado** em
        `tls_enforcement_test`: o arquivo faz 6 `put_env(:enable_tls)` sem `on_exit` de restauração — passa
        isolado; fix num commit à parte); credo/dialyzer limpos. **Camada B do cliente concluída.**
      - ✅ **Painel NorthGuard no dashboard (com drill-down on-demand).** Devolve, no modelo certo, a
        visibilidade que o trim do B3b-i tirou, em **dois níveis** para o stream ficar leve. Funções **puras**
        no `Metadata`: `overview/1` (resumo por topic — estado/keyspace/política, contagens de ranges e
        segments, bytes totais, grupos consumidores) e `topic_detail/2` (o drill-down de **um** topic — seus
        ranges, cada um com seus segments; `nil` se o topic não existe). Ambas reusam `ranges_of_topic`/
        `segments_of_range` e achatam os ids de tupla p/ JSON. O `/metrics` e o `/stream` (1s) mandam só o
        **resumo** (via `dashboard_metrics/0`); o detalhe é buscado **on-demand** por topic no novo endpoint
        `GET /topic?name=` (autenticado como o `/metrics`; `serve_json/3` DRY; a query string agora é
        preservada no roteamento). No front, expandir um topic dispara um `fetch` único (`loadTopicDetail`,
        cacheado em `topicDetails`, mostra "loading…" até chegar); o cabeçalho-resumo segue vivo pelo stream.
        Nomes de topic/grupo **escapados** (XSS — `escapeHtml`; toggle por índice; URL via
        `encodeURIComponent`). Testado: `overview/1`+`topic_detail/2` unit (4); e2e de `/metrics` (resumo) e
        `/topic` (detalhe + 404); functional-check do fluxo on-demand em node (fetch encodado, loading,
        render pós-fetch, escaping). Assim o tradeoff do stream-full sumiu: zero tráfego de segment a não ser
        no topic expandido. 790 testes, 0 falhas; credo/dialyzer limpos.
      - ✅ **Cap de tamanho de frame no protocolo binário (fix de DoS).** Achado ao investigar o `Validator`
        órfão: o caminho binário **não tinha teto de frame** — o enforcement de tamanho saiu junto com o
        modelo de fila no B3b e o binário nunca o replicou. O `tcp_acceptor` acumulava `buffer <> data` até
        `Wire.decode_frame` casar (`<<len::32, body::binary-size(len)>>`, sem teto), então um cliente podia
        anunciar `len = 4 GB` e forçar buffer ilimitado. Novo `Wire.decode_frame/2` rejeita
        `{:error, :frame_too_large}` **assim que os 4 bytes do length-prefix chegam** (antes de bufferizar o
        corpo); o acceptor aplica em todos os 3 pontos de leitura (auth, request loop, streaming), respondendo
        um erro e fechando. Teto configurável (`:max_frame_size`, default 16 MiB). Testes: frame gigante
        rejeitado (pós-auth e no handshake) sem bufferizar + servidor sobrevive; boundary (com cap reduzido:
        frame no teto processa, 1 byte acima rejeita). 793 testes, 0 falhas; credo/dialyzer limpos.
      - ✅ **Remoção do `Malachi.Validator` órfão.** Feito o fix de DoS (o único hardening real que ele fazia
        agora vive no lugar certo, o boundary do wire), o `Validator` inteiro estava morto — 7 funções com
        **0 callers vivos**, um GenServer supervisionado com ETS que ninguém usava (o modelo de fila era o
        único cliente; o caminho binário valida topic name no próprio `Metadata.valid_topic_name?`).
        Deletados: `validator.ex` + sua entrada de supervisão; as 3 métricas de validação do `Metrics`
        (`increment_validation_error`/`cache_hit`/`cache_miss`) + a seção `validation` de `get_system_metrics`;
        e o config morto de runtime.exs (bloco de name/header validation do Validator + o bloco resource/
        backpressure e `max_dynamic_*`, resquícios do modelo de fila — todas com 0 readers). Testes: deletados
        os puros de Validator (`validator_test`, `injection_attack_test`, `input_fuzzing_test`); adaptados os
        mistos (`atom_safety` → só AtomMonitor; `attack_simulation` → removido o teste de nome via Validator;
        `security_performance_regression` → removidos os benchmarks de Validator, mantidos Auth/RateLimiter/
        ConnectionLimiter/lockout). Cobertura viva intacta: o caminho binário já é fuzzado pelo
        `binary_protocol_security_test`. 685 testes, 0 falhas; credo/dialyzer limpos (−4 arquivos).
      - ✅ **Observabilidade (A: Prometheus+health/ready · B: telemetry · C: OTel — cada fatiado).**
        - ✅ **O1 — health/readiness.** Endpoints HTTP **sem auth** na porta do dashboard (probes não
          autenticam): `GET /health` (liveness, sempre 200 `{"status":"ok"}`) e `GET /ready` (readiness:
          200 `{"status":"ready"}` se o `LogBroker` está vivo, senão 503 `not_ready` — pra um LB/k8s parar
          de rotear a um nó ainda bootando ou sem broker). Adicionados a `is_public_route` (bypass de auth)
          + `serve_status/4` (código variável). Testado: happy path (dashboard_test) e a **propriedade-chave**
          (dashboard_security: 200 sem token mesmo com auth habilitado). README com exemplo de probes k8s.
        - ✅ **O2 — endpoint Prometheus.** Módulo **puro** `Malachi.Metrics.Prometheus` (`export(system,
          topics) → iodata`) renderiza o formato-texto de exposição v0.0.4 a partir do `get_system_metrics`
          + `Metadata.overview`: health do BEAM (process/memory/uptime/io/atom), contadores de segurança
          (rate-limit por ação, failed-auth, lockouts, dashboard-auth, TLS handshakes) e gauges por-topic
          (ranges/segments/bytes/grupos). O `/metrics` do dashboard virou **content-negotiated**: `Accept:
          text/plain`/`openmetrics` → texto Prometheus, senão o JSON de sempre (preserva dashboard + o teste
          JSON) — mesma auth (any-user; scraper passa token). Labels escapados (defensivo). Testado: unit do
          módulo (HELP/TYPE, labels, valores int/float, escaping, sem topics) + e2e (Accept: text/plain →
          exposição com `malachi_up` e o gauge do topic criado). 695 testes, 0 falhas; credo/dialyzer limpos.
        - ✅ **O3 — eventos `:telemetry` nos hot paths.** Dep `:telemetry` + módulo `Malachi.Telemetry`
          (catálogo + wrappers, DRY) emitindo em: **produce** (`LogApi.produce_records` → `%{count, bytes}`/
          `%{topic}`), **consume** (`LogApi.do_fetch` → `%{count}`/`%{topic}`), **auth** (`Auth.authenticate/3`
          refatorado num wrapper fino → `%{count:1}`/`%{result: :ok|:error}`), e **replicação** (`ReplicationServer`
          no reply do quórum → `%{count}`/`%{result: :ok|:no_quorum}`). Emitir é no-op quando nada está
          anexado (seguro no hot path). Testado: handler anexado + produce/consume/auth (+ o commit de
          replicação que o single-node dispara). Achado no caminho: um **flake pré-existente** no
          `connection_limiter_test` (limite **global** — estado compartilhado entre testes; passa isolado,
          não relacionado ao O3) → registrado p/ fix à parte. 698 testes (credo/dialyzer limpos).
        - ✅ **O4 — handler default telemetry → Metrics.** `Malachi.Telemetry.MetricsReporter` anexa (idempotente,
          no `Metrics.init`) aos 4 eventos e dobra cada um em contadores ETS: produce (`records_produced`+
          `bytes_produced`), consume (`records_consumed`), auth (`{:auth_result, :ok|:error}`), replicação
          (`{:replication_result, :ok|:no_quorum}`). `get_system_metrics` ganha a seção `operations`, e o
          Prometheus emite `malachi_records_produced_total`/`bytes_produced_total`/`records_consumed_total`,
          `malachi_auth_attempts_total{result}` e `malachi_replication_commits_total{result}` — assim o
          endpoint do O2 ganha throughput/auth/replicação sem cada operador escrever handler (podem anexar os
          seus ao lado). Testado: reporter e2e (evento → contador via `get_system_metrics`) + o Prometheus com
          a seção. 700 testes, 0 falhas; credo/dialyzer limpos. **Bloco A+B da observabilidade concluído.**
        - ✅ **O5 — OpenTelemetry tracing (bloco C; C-lite → C-full).** Tradeoff registrado: OTel é pesado
          (deps + precisa de collector) e os eventos do O3 já são base; decisão **C-lite primeiro**.
          - ✅ **O5a — spans nas operações do cliente.** Deps `opentelemetry_api`+`opentelemetry` (API 1.5 +
            SDK 1.7; sem exporter/grpcbox — footprint enxuto). Tracing **off por default** (`sampler:
            :always_off` + `traces_exporter: :none`) → o `with_span` no hot path é **no-op**, zero custo por
            operação até o operador optar (sampler `:always_on` + exporter OTLP). O `LogApi` embrulha
            `produce_records`/`do_fetch` em `Tracer.with_span` (`malachi.produce`/`malachi.consume`) com
            atributos `malachi.topic`/`records`/`bytes`. Sem propagação cross-process ainda (span por
            operação, raiz). Testado: comportamento intacto + **captura real de span** (test.exs usa `sampler:
            :always_on` + o `simple` processor + `otel_exporter_pid`; assere nome + atributos via record do
            header OTel + `otel_attributes.map`). 702 testes, 0 falhas; credo/dialyzer limpos.
          - ✅ **O5b — propagação de contexto cross-process/cross-node.** O trace do produce agora atravessa
            os processos: `BrokerServer.produce` captura o `otel_ctx` do caller (o span `malachi.produce` do
            `LogApi`) e o passa na mensagem do GenServer; o `handle_call` **anexa** o ctx e embrulha o trabalho
            num span filho `malachi.broker.produce`. Idem no hop mais fundo: `ReplicationServer.replicate`
            captura o ctx (agora o span do broker) e o passa (mensagem virou 6-tupla nas 3 clauses); o
            `handle_call` anexa + span `malachi.replication.commit` — como o ctx é um mapa serializável, isso
            **linka cross-node** (produce num nó, replicação no primário remoto, mesmo trace). Tracing off por
            default → `get_current`/`attach`/`detach` são pdict-ops baratas quando não há span. Testado: um
            produce gera os 3 spans com o **mesmo trace_id** e a cadeia de parent correta
            (produce → broker.produce → replication.commit). 703 testes, 0 falhas; credo/dialyzer limpos.
            **Observabilidade (A/B/C) concluída.**
  - ✅ **Cliente de referência (Node.js) reformulado para a nova arquitetura.** Os scripts Node.js antigos
    falavam o protocolo JSON de fila/canal (removido no B3b); reescritos para o **protocolo binário
    (`Malachi.Wire`) + modelo de log** (topic/key/cursor opaco). Estrutura: `scripts/lib/wire.js` — port
    fiel do codec (framing length-prefixed, envelope request/response, `put_str` com flag de presença,
    records **sem offset**, todos os payloads das 7 operações); `scripts/lib/client.js` — conexão TCP que
    **multiplexa requests por `correlation_id`** e roteia os frames de **push** (o servidor reusa o corr_id
    do `subscribe`) para o callback da subscription, não para um request one-shot; `scripts/lib/cli.js` —
    cores/config-de-env/parse-de-args compartilhados (DRY, os scripts antigos duplicavam). CLIs: `producer.js`
    (append por chave, `--create`/`--key`/`--continuous`), `consumer.js` (pull dirigido por cursor;
    `--group` resume + commita server-side; `--follow` long-poll), `subscriber.js` (server-push streaming
    subscribe+ack com janela de crédito — substitui o `channel-*` pub/sub, que sumiu com o modelo de canal).
    **Deletados:** `channel-publisher.js`/`channel-subscriber.js`/`channel-demo.sh` (modelo de canal
    removido) e `i18n.js` (órfão; os novos scripts usam strings inglesas inline). `channel-demo.sh` virou
    `streaming-demo.sh` (append → stream ao vivo). Validado e2e contra o servidor real: auth →
    create_topic → produce → fetch-por-cursor (avança/drena) → commit+resume-de-grupo (2ª run consome 0) →
    streaming push+ack (pré-existentes + ao vivo), e caminhos de erro limpos (`permission_denied`,
    `invalid_credentials` — sem crash). README com a seção do cliente; `package.json` atualizado (v2, scripts
    produce/consume/subscribe/demo). **Sem dependências** (só `net` da stdlib).
  - ✅ **Load-test harness (Node.js, closed-loop).** `scripts/loadtest.js` gera carga sobre o cliente de
    referência (escolhas: 1A Node reusando o cliente · 2C closed-loop agora, estruturado p/ open-loop depois
    · 3A os quatro cenários). N conexões concorrentes, cada uma num loop `op → await` até o deadline; o driver
    `closedLoop` é **separado das ops** (`produceOp`/`fetchOp`) pra um driver open-loop reusá-las. Cenários:
    **produce** (append por chave), **fetch** (drena backlog dirigido por cursor, rebobina no fim), **stream**
    (server-push subscribe+ack, throughput-only — latência de push não é comparável a round-trip), **mixed**
    (metade produz, metade lê). Métricas: throughput (ops/s, records/s, MB/s) + latência p50/p90/p95/p99 via
    **reservoir sampling** (Algoritmo R, cap `--samples`) com min/max/count/sum exatos à parte (o reservoir
    clipa a cauda). Flags: `--connections/--duration/--batch/--record-size/--keys/--max/--window/--prepopulate/
    --warmup/--json`; tópico único auto-criado por run; `--prepopulate` semeia backlog p/ fetch/stream/mixed.
    Três correções achadas na revisão/validação: (1) backoff no erro não-fatal do `closedLoop` (senão
    busy-spin pegando CPU); (2) `clearTimeout` no `streamDriver` quando `onError` resolve antes; (3) **grupo
    único por invocação** no stream — o warmup commita (ack) até o fim do backlog, então compartilhar grupo
    com o run medido o deixava vazio. Validado e2e (single-node local, records pequenos, ilustrativo):
    produce ~42k rec/s, fetch ~307k rec/s (drain), stream ~26k rec/s (push), mixed ~25k rec/s / 7.5k ops/s —
    0 erros; `--json` e `--warmup` (com reconexão dos clients p/ soltar a subscription) OK. Nota operacional:
    muitas conexões estouram o rate-limit de auth (10/min/IP default) — subir `MALACHIMQ_AUTH_RATE_LIMIT` pra
    testes de escala. README com a seção de load test; `package.json` com o script `loadtest`.
    - ✅ **Driver open-loop (o 2C).** `--rate <rps>` dispara requests a uma **taxa de chegada fixa**,
      independente de respostas anteriores, medindo a latência a partir do **tempo agendado** de cada request
      (não do envio real) — **correção de coordinated omission**: um stall do servidor aparece como latência
      alta nos requests que empilharam atrás, o que o closed-loop esconderia (o worker parado não emite). O
      driver `openLoop` reusa as mesmas ops (`scenarioOps` extraído, DRY entre os dois drivers); fetch fica
      stateless (ctx novo por request → lê do início). Requests espalhados round-robin no pool (o cliente
      multiplexa por corr_id). Guard de memória: `--max-inflight` (default 100k) — ao atingir, para de
      empilhar e **flag `saturated`** (servidor não sustenta a taxa). Report ganha modo, alvo vs. atingido,
      saturação e rótulo CO-corrected (texto + JSON). Não se aplica a `stream` (push; `--rate` ignorado com
      aviso). Validado e2e: taxa **sustentável** (1200 rps → atingiu 1199, latência estável p50 1.5ms);
      **sobrecarga** (5000 rps acima da capacidade single-node → latência CO explode, mean 2.6s/p99 5.1s, o
      sinal que o closed-loop mascara); **guard** (`--max-inflight 200` flagou saturação e parou); closed-loop
      inalterado. Três correções da revisão anterior seguem (backoff, timer, grupo único). README/help
      atualizados.
  - ✅ **Deploy multi-nó/replicado (incremental: D1 → D2 → D3).** As peças de HA já existiam e eram
    testadas isoladamente (SWIM membership, replicação por quórum cross-node, `ra`, self-healing,
    failover); esta fase **liga-as na aplicação**. Descoberta de nós **estática via config** (o SWIM
    detecta falhas em runtime; `libcluster` fica para depois).
    - ✅ **D1 — Control plane HA (metadata via `ra`).** `application.ex` sobe o `Malachi.LogBroker` com
      `metadata_cluster`/`metadata_nodes` quando `:log_cluster` está configurado (env
      `MALACHIMQ_LOG_CLUSTER`/`MALACHIMQ_LOG_NODES`/`MALACHIMQ_RA_DATA_DIR`), iniciando `ra`; **ausente
      = single-node in-memory** (default preservado). A decisão config→opções é uma função pura
      testável (`Application.metadata_cluster_opts/2`). O mecanismo (metadata sobrevive à perda do
      líder) já é provado por `metadata_ha_test`/`broker_server_ra_test` — não duplicado. Também
      **isolado o `log_data_dir` por execução de teste** (`config/test.exs`): o dir fixo persistia entre
      runs e, com metadata in-memory reiniciando, um topic reusado colidia com um segment em disco
      (`Log.ensure_active :already_exists`) → flakiness e2e; agora cada run usa um dir próprio, limpo no
      `after_suite`.
    - ✅ **D2 — Data plane replicado.** Em modo cluster, `application.ex` sobe um `ReplicationServer`
      **nomeado** (`Malachi.LogReplication`) por nó e liga o `LogBroker` a `brokers: [{Malachi.LogReplication,
      n} | n ← log_nodes]` + `replication_factor` (env `MALACHIMQ_LOG_REPLICATION_FACTOR`, default 3,
      clampado ao nº de nós pelo broker). O `Placement` (HRW) escolhe o `replica_set` entre esses brokers;
      o primário replica cross-node via `{name, node}` e commita por **quórum**. Broker set **estático**
      (todos os nós da config); um follower caído é tolerado pelo quórum (o `live_brokers` ao vivo é D3).
      Fiação testável por funções puras (`Application.broker_refs/1`, `data_plane_opts/2`); o mecanismo de
      quórum/tolerância já é coberto por `replication_server_test`, e a integração (BrokerServer + 3 brokers
      + rf=3 + ra → produce/consume por quórum ponta a ponta) por `broker_server_ra_test`.
    - ✅ **D3 — Membership + healing/failover ao vivo.**
      - ✅ **D3a — `MembershipServer` cross-node.** O SWIM identificava cada membro pelo `self_ref`, que
        era o `:name` de registro — os testes eram todos in-process (átomos únicos). Cross-node isso
        colidia: o mesmo átomo `Malachi.LogMembership` resolve para o servidor **local** em cada nó, então
        o remetente gossipado apontava para o próprio receptor e a view não convergia. Agora o
        `MembershipServer` aceita `:self_ref` (identidade **node-qualified** `{name, node()}`, gossipada)
        distinto do `:name` (registro local), + `start/1` (não-linkado, p/ iniciar em nós remotos). Provado
        por **teste multinode** real (`membership_ha_test`): 3 nós `:peer` convergem via seeds e detectam a
        morte de um nó (SWIM: suspeita → dead → sai do alive set).
      - ✅ **D3b — Fiação na app.** Em modo cluster, `application.ex` sobe (nesta ordem)
        `MembershipServer` → `ReplicationServer` → `LogBroker` → `HealCoordinator`. O `MembershipServer`
        usa `self_ref: {Malachi.LogMembership, node()}` e `peers: membership_seeds(log_nodes)` (os outros
        nós). O `live_brokers` (fun) deriva de `alive_members` → refs `{Malachi.LogReplication, node}`, e é
        passado ao `LogBroker` (que estreita o placement de novos segments ao conjunto vivo; `:brokers`
        estático é o placement inicial) **e** ao `HealCoordinator`. O `HealCoordinator` (`metadata_source:
        BrokerServer.metadata`, `apply_command: BrokerServer.apply_heal`, `replication_factor`) fecha o loop
        *broker morre → membership marca gone → segments re-replicados + primário promovido*. Fiação pura
        testável (`membership_seeds/1`, `live_replication_refs/1`); o loop de healing/failover já é coberto
        por `heal_coordinator_test`/`self_healing_test`/`failover_test`, e o membership cross-node por D3a.
  - ✅ **Deploy multi-nó/replicado completo** (D1 control plane HA + D2 data plane replicado + D3
    membership/healing/failover ao vivo). Ligado por config estática; `libcluster` (descoberta dinâmica)
    fica como conveniência futura.
- ✅ **C — Features NorthGuard restantes.** Decisão: começar por **C1 — retenção (tempo+tamanho)**;
  attributes (C2) e policies (C3) depois. Design aprovado: `sealed_at` explícito no segment (idade),
  retenção por tamanho **por range**, e consumidor num dado expirado **avança para o início disponível**
  (mantém o cursor opaco). Sub-fatias: C1a (primitiva de delete) → C1b (coordenador + política + fiação).
  - ✅ **C1a — control plane.** `segment_meta` ganha `sealed_at` (epoch ms, `nil` enquanto ativo); o
    comando `seal_segment` carrega o `sealed_at` (gerado no `Broker` como os timestamps de `Record`, então
    determinístico entre réplicas). Novo comando `{:delete_segment, segment_id}`: remove um segment
    **selado** do control plane (`:segment_active` se ainda ativo — nunca dropa o ativo; `:no_such_segment`
    se ausente). Testado: unit do `delete_segment` (selado/ativo/inexistente), `sealed_at` no seal,
    determinismo preservado (property tests).
  - ✅ **C1a — storage delete.** Cada segment do broker é um `Log` num subdiretório próprio, e o
    `ReplicationServer` guarda `logs: %{segment_id => Log}`; então deletar é **fechar o `Log` + apagar o
    diretório** — sem precisar de delete granular no `SegmentStore`. `Log.delete/1` (fecha + `rm_rf` do
    diretório, best-effort). `ReplicationServer.delete/2` (client + handle_call): fecha/apaga se o log
    está aberto, senão limpa arquivos órfãos em disco (pós-restart); **idempotente** (deletar um segment
    desconhecido é `:ok`). Testado: `Log.delete` (diretório some), `ReplicationServer.delete` (dados
    somem → read vira `:eof`; idempotência).
  - ✅ **C1b — coordenador + read path + fiação** (incremental).
    - ✅ **C1b-1 — política + `RetentionCoordinator`.** `segment_meta` ganha `byte_size` (via `seal_segment`,
      do `active.bytes` do Broker — determinístico, como `sealed_at`; retenção por tamanho precisa de bytes).
      Módulo **puro** `Malachi.Cluster.Retention`: `expired(metadata, now_ms, policy)` → ids de segments
      **selados** a expirar por **idade** (`sealed_at` > `max_age_ms`) e por **tamanho** (soma `byte_size`
      por **range** > `max_bytes` → mais antigos primeiro), unidos; nunca o ativo; bound `nil` desliga a
      regra. `RetentionCoordinator` (GenServer periódico, modelo `HealCoordinator`) com seams
      (`metadata_source`, `expire_segment`, `policy`, `clock`, `interval`): cada sweep resolve os ids para
      seus metas e chama `expire_segment`. Testado: `Retention` (idade, tamanho por-range, união, nunca o
      ativo, `nil` desliga) e o coordenador (sweep via `run_now` e via tick, meta completa ao seam).
    - ✅ **C1b-2 — read path (avança em dado expirado).** Retenção deleta sempre um **prefixo contíguo**
      (os segments mais antigos), então o read path só precisa saber o menor `start_offset` ainda armazenado
      (`earliest_offset/2`) e **clampar o offset a ele** antes de ler. `consume_page` (consumo ao vivo) e
      `read_history_page` (admin) clampam: um consumidor cujo cursor caiu num buraco expirado avança
      transparentemente para o dado vivo (at-least-once, cursor opaco intacto) em vez de ver `:eof` enganoso;
      como não há buracos internos, `offset + length` continua correto (sem mudar `read`/`locate_segment`).
      Testado: consumidor abaixo do earliest pula os segments expirados e lê o que resta.
    - ✅ **C1b-3 — config + fiação (retenção C1 completa).** `Broker.delete_segment/2` +
      `BrokerServer.delete_segment/2` (aplicam `:delete_segment` pelo control plane, Raft-backed quando
      configurado). A `expire_segment` real (na `application.ex`): remove do control plane e então deleta
      o storage em cada réplica (`ReplicationServer.delete`), **best-effort** (control plane idempotente,
      storage tolera segment ausente). Config por env: `MALACHIMQ_RETENTION_MAX_AGE_MS` /
      `MALACHIMQ_RETENTION_MAX_BYTES` (ambos ausentes = **guarda para sempre**, coordenador não sobe) /
      `MALACHIMQ_RETENTION_INTERVAL_MS`. O `RetentionCoordinator` sobe na árvore **sempre que há política**
      (importa single-node também), após o `LogBroker`. Testado: `BrokerServer.delete_segment` (drop do
      selado), e **e2e** (produce → sela → sweep do coordenador → segment some do control plane **e** do
      storage). **C1 (retenção tempo+tamanho) completa.**
- ✅ **C2 — Attributes** (k/v opacos que o admin liga a brokers; base de rack/DC-awareness).
  **Decisão:** disseminar via **Membership/SWIM** (fiel ao NorthGuard: "membership piggyback host/port/
  attributes"), não no Metadata — o usuário priorizou fidelidade. Incremental: C2a (Membership puro) →
  C2b (server + API + gossip) → C2c (fiação + config).
  - ✅ **C2a — `Membership` puro com attributes.** Os attrs de um membro **viajam com o update**,
    governados pela mesma **incarnation**: o `update` vira 4-tupla `{member, status, incarnation,
    attributes}` e o `member_state` ganha `attributes`. Só o dono muda seus attrs — via `set_attributes/2`,
    que **sobe a própria incarnation** para a mudança vencer o merge em todo lugar; uma suspeita/confirmação
    de outro nó carrega os attrs **já conhecidos** (preserva-os). O `overrides?` (precedência `{incarnation,
    rank}`) não muda. `new/2` aceita `:attributes` do self; query `attributes/2`. Testado: propagação/troca
    por incarnation, `set_attributes`, preservação em suspect, gossip via `updates`; convergência
    order-independent preservada (property; attrs são consistentes por-incarnation, então os generators
    usam `%{}`). Multinode SWIM (D3a) segue convergindo com a 4-tupla.
  - ✅ **C2b — `MembershipServer` com attributes.** Opção `:attributes` (attrs iniciais do self,
    passados ao `Membership.new`); API `set_attributes/2` (muda os próprios attrs em runtime — sobe a
    incarnation) e `attributes/2` (lê os de um membro). Disseminação é **passiva** (anti-entropy): o
    server ignora os effects e o gossip periódico (ping/ack piggyback `updates`) propaga — nenhum push
    proativo, consistente com o resto do server. Testado: attrs iniciais legíveis, e `set_attributes` num
    nó propaga a um peer via gossip.
  - ✅ **C2c — fiação na app (C2 completa).** `application.ex` liga os attributes do self no
    `MembershipServer` do cluster: `MALACHIMQ_LOG_ATTRIBUTES` (formato `"rack=a,dc=east"`) é parseado por
    `Application.parse_attributes/1` (função pura testável: ignora entradas sem `=`, trima, preserva `=` no
    valor) e passado como `:attributes`. Ausente → `%{}`. Testado: parse (vazio, pares, trim, entradas
    inválidas, valor com `=`). **C2 (attributes via SWIM) completa** — os brokers disseminam seus attrs por
    gossip, prontos para o placement rack-aware de C3.
- ✅ **C3 — Policies** (nome + retenção + constraints sobre attributes → replica sets; fiel ao NorthGuard,
  que unifica tudo em *policies*). Incremental: C3a (Placement puro com spread) → C3b (integração: attrs do
  membership → placement) → C3c (policies por-topic: definição + associação + retenção por-topic).
  - ✅ **C3a — `Placement` puro com spread (rack-aware).** `place/4` ganha a opção `:spread =
    {attribute_key, attributes}`: sobre o ranking HRW (determinístico), faz **round-robin pelos valores
    distintos** do attribute — o melhor-rankeado de cada valor primeiro, depois o próximo de cada, até `rf`.
    Com `rf ≤ nº de valores`, cada réplica num rack/DC distinto; senão best-effort round-robin. Determinístico
    (ranking HRW + agrupamento estável); sem `:spread` → top-`rf` de antes (todos os callers de `place/3`
    intactos). Testado: valores distintos, prioriza diversidade sobre rank puro (rf=2 em a,a,b → a,b),
    best-effort com rf > valores, brokers sem o attribute agrupam à parte, determinismo.
  - ✅ **C3b — integração (attributes → placement).** O `Broker` ganha `spread_by` (a chave de attribute)
    e `broker_attributes` (map broker→attrs); `open` os aceita, `set_broker_attributes/2` os atualiza (como
    `set_brokers`), e `open_segment` passa `:spread` ao `Placement.place` quando `spread_by` está setado
    (senão placement normal — todos os callers intactos). O `BrokerServer` aceita `:spread_by` + uma fun
    `:broker_attributes` e a **refresca periodicamente** (mesmo timer de `:live_brokers`) para o broker —
    então os attrs disseminados pelo membership (C2) fluem ao placement. Testado: produce espalha réplicas
    por rack, `set_broker_attributes` afeta o placement seguinte, refresh do `BrokerServer` puxa os attrs.
  - ✅ **C3c-1 — fiação do rack-aware na app.** `data_plane_opts` liga `spread_by` (env
    `MALACHIMQ_LOG_SPREAD_BY`, ex: `"rack"`) e uma fun `broker_attributes` derivada do `MembershipServer`:
    `broker_attributes_for/2` (pura, testável) mapeia cada membro vivo `{LogMembership, node}` →
    `{LogReplication, node}` com os attrs gossipados (C2). Com isso o **placement rack-aware funciona ponta
    a ponta na aplicação** (attrs do membership → spread do placement). Ausente `spread_by` → HRW puro.
  - ✅ **C3c-2 — policies por-topic** (o guarda-chuva NorthGuard). Decisão: policies **dinâmicas no
    Metadata (`ra`)** + escopo **ambos** (retenção + placement por-topic). Incremental: 2a (Metadata:
    policies + associação) → 2b (retenção por-topic) → 2c (placement por-topic). **Fecha C3.**
    - ✅ **C3c-2a — Metadata: policies + associação.** `Metadata` ganha `policies: %{name => policy}`
      (`policy = %{optional(:retention) => %{max_age_ms, max_bytes}, optional(:spread_by) => term}`), o
      `topic_meta` ganha `policy: name | nil`, e dois comandos: `{:define_policy, name, policy}` (valida
      name binário não-vazio + policy map; `:invalid_policy` senão) e `{:set_topic_policy, topic,
      name | nil}` (`:no_such_topic`/`:no_such_policy`; `nil` desassocia → volta aos globais). Queries
      `get_policy/2` e `topic_policy/2` (resolve a policy do topic). Determinismo preservado (property).
      **Sem uso ainda** — 2b/2c ligam retenção/placement à policy do topic.
    - ✅ **C3c-2b — retenção por-topic.** `Retention.expired/3` passa a resolver, **por range**, a retenção
      efetiva = a `:retention` da policy do topic (`Metadata.topic_policy/2`) **mesclada sobre** a policy
      global (a policy sobrepõe só as chaves que define; `Map.merge`), ou a global quando o topic não tem
      policy. O `RetentionCoordinator` não muda (já passa o metadata + a global). Testado: policy do topic
      sobrepõe a global, merge (chave não-definida cai na global), topic sem policy usa a global.
    - ✅ **C3c-2c — placement por-topic.** `Broker.open_segment` resolve, por range, o `spread_by`
      efetivo = o `:spread_by` da policy do topic (`Metadata.topic_policy/2`) quando a policy **define**
      essa chave (`nil` explícito opta o topic **fora** do spread — rendezvous puro), sobrepondo o global;
      senão o `spread_by` global do broker. Simétrico ao 2b (chave definida vence, `nil` incluso). Só
      `place_opts/effective_spread_by` mudam. Testado: policy liga o spread sobre um global-off; `nil`
      explícito desliga sobre um global-on (== rendezvous puro).
- ✅ **D — Sharding via `ReplicatedDSRSM`** (agora **no alvo**): metadata sharded (um cluster `ra`
  por vnode) para **escalar o control plane** além de um cluster Raft único. Decisão: **1A** — o cache
  do `Broker` vira um `DSRSM` (espelha o par `Metadata`/`ReplicatedMetadata`), roteando leituras/escritas
  por topic; e **2A** — incremental, núcleo puro primeiro. Infra já pronta: `HashRing`, `DSRSM` (puro),
  `ReplicatedDSRSM` (ra), `MetadataMachine`/`MetadataServer`.
  - ✅ **D-a — `Broker` sobre `DSRSM` (in-memory, 1 vnode).** O cache do `Broker` deixa de ser um
    `Metadata` e passa a ser um `DSRSM` (`broker.dsrsm`), roteado por topic (derivável do `range_id`
    `{topic, seq}`/`segment_id`). Novo combinador puro `DSRSM.update_vnode/3` (roteia + aplica uma
    função ao `Metadata` do vnode); `DSRSM.command/3` delega a ele com `&Metadata.apply/2` (property
    tests intactos). `command_fun` do `Broker` vira `(DSRSM, topic, command) -> {DSRSM, reply}` (default
    `&DSRSM.command/3`); `apply_metadata` deriva o topic via `command_topic/1`. Acessores novos no
    `DSRSM`: `single/1` (forma trivial 1-vnode p/ seed), `committed_offsets/3`, `topic_policy/2`,
    `merged_metadata/1` (união dos shards → `Broker.metadata/1` p/ retention/healing). No `BrokerServer`,
    o caminho Raft embrulha o cluster único como `DSRSM.single(seed)` + um `command_fun/3` que injeta
    `ReplicatedMetadata.apply_command` no `update_vnode` (D-b troca pelo `ReplicatedDSRSM` real). Com 1
    vnode, comportamento idêntico: suite completa verde (981) como rede de segurança.
  - ✅ **D-b** (runtime/`BrokerServer` sobre `ReplicatedDSRSM`, N vnodes). Decisão: **1A** — D-b-1
    (N vnodes **single-node**) primeiro; HA-por-vnode (D-b-2) depois.
    - ✅ **D-b-1 — control plane sharded single-node.** `BrokerServer` ganha o caminho `:metadata_vnodes`
      (`[{cluster_name, token}]`): inicia N clusters `ra` via `ReplicatedDSRSM` (um por vnode), materializa
      o cache local com `ReplicatedDSRSM.snapshot/1` (novo — lê o `Metadata` de cada vnode → `DSRSM.seed/2`,
      novo, compartilhando o ring), e um `command_fun/3` sharded que roteia por topic ao cluster `ra` do
      vnode (`ReplicatedDSRSM.server_for/2`, novo) aplicando via `ReplicatedMetadata.apply_command` no
      `update_vnode`. O caminho `:metadata_cluster` (1 vnode, D-a/D1) segue intacto. Config: `log_vnodes`
      (inteiro N; `Application.sharded_vnodes/2` gera N vnodes com tokens uniformes no ring de 32 bits).
      Testado: 2 vnodes, cada topic vive em exatamente o cluster `ra` que seu nome roteia (nunca no outro),
      e os topics se distribuem entre os vnodes; `sharded_vnodes/2` (tokens distintos, em range).
    - ✅ **D-b-2 — HA por vnode.** `ReplicatedDSRSM.add_vnode/4` passa a receber os `nodes`, iniciando o
      cluster `ra` de cada vnode sobre eles (`MetadataServer.start/2`), então cada vnode sobrevive à perda
      de um membro. Modelo: **todos os vnodes sobre o mesmo conjunto de M nós** (espelha o D1; placement de
      vnodes por subconjuntos de nós fica para depois). `BrokerServer` passa os `metadata_nodes` ao caminho
      sharded (`start_vnodes/2`); `Application.metadata_opts` inclui `metadata_nodes` no caminho sharded.
      `snapshot/1` usa `&Function.identity/1` (query linearizável roda no líder, possivelmente remoto).
      Testado (`:multinode`): 2 vnodes sobre 3 nós, mata um membro do vnode dono (o líder se for peer →
      failover; senão um follower), o vnode ainda commita e os metadados (dele e do outro vnode) intactos.
  - ✅ **D-c — gestão do control plane por vnode** (retention/healing/failover). **Concluído por 1C-a +
    1C-b** (coordinators só-no-líder + manager per-vnode-leader; ver as sub-fatias abaixo). O texto a
    seguir é o **contexto do débito** que motivou 1C — o estado *antes* de 1C. **Estado pré-1C:** as
    *escritas* de metadata já eram sharded (D-b), mas a *gestão* seguia **centralizada** — um
    `RetentionCoordinator` e um `HealCoordinator` no nó do `BrokerServer` leem `merged_metadata` (a
    **união** de todos os shards) e emitem comandos (`delete_segment`/`set_segment_replicas`) que
    **roteiam de volta** por topic ao vnode dono (via `command_topic/1`). Isso é **correto** sob
    sharding (a união é exata; os comandos roteiam), mas reintroduz conceitualmente o ponto único que
    o sharding elimina — um **débito de fidelidade de sequenciamento**, não de correção.

    O alvo fiel ao NorthGuard é **1C: um coordinator vivendo na liderança do grupo Raft de cada vnode**
    (cada nó gerencia retention/healing dos vnodes que lidera). Isso **pertence à Fase 1** (distribuição),
    **não** à Fase 2 (eficiência nativa/profiling). O motivo de 1C não vir já não é ser "otimização",
    e sim ter **pré-requisitos**:
      1. **Placement de vnodes por subconjuntos de nós** (hoje todos os vnodes vivem nos mesmos M nós —
         adiado em D-b-2). Sem espalhar os vnodes, "o líder do vnode" é qualquer um dos M nós e há pouca
         distribuição real a fazer. **Fatia D-c-1** (decisão: **1A** HRW reusando `Placement`; **2A**
         núcleo puro primeiro):
           - ✅ **D-c-1a — núcleo puro.** `Application.place_vnodes/3` atribui a cada vnode
             (`{vnode_id, token}`) os `R` nós do seu cluster `ra`, escolhidos de `nodes` por rendezvous
             (o mesmo HRW `Placement.place/4` dos segments) → `{vnode_id, token, nodes}`; determinístico,
             mínimo movimento, `R` efetivo = `min(R, M)`. Testado isolado (HRW espalha, determinismo,
             clamp). **Sem uso ainda** — D-c-1b liga ao `ReplicatedDSRSM`/`BrokerServer`.
           - ✅ **D-c-1b — roteamento cross-node.** `MetadataServer.start/2` passa a devolver o server de um
             **membro real** (o nó local quando é membro; senão o primeiro do placement) em vez do local
             sempre — então um vnode colocado num subconjunto de nós é alcançável de um nó que **não** hospeda
             réplica dele (o `ra` roteia `command`/`query` desse membro ao líder; o chamador não precisa ser
             membro). `ReplicatedDSRSM` armazena esse server; `command`/`query`/`snapshot`/`server_for`
             passam a funcionar cross-node. Testado (`:multinode`): 2 vnodes em subconjuntos **disjuntos** de
             3 nós, orquestrados de um nó que **não** hospeda nenhum — commits/queries roteiam ao membro certo
             e o `snapshot` materializa tudo. (Decisão **1A**: mecanismo isolado do bootstrap distribuído.)
           - ✅ **D-c-1c — bootstrap distribuído (seed estático).** `Application.metadata_opts` liga o
             `place_vnodes` (`metadata_vnodes` vira `[{vnode_id, token, nodes}]`, R = `log_vnode_replication_factor`)
             e injeta a política `bootstrap_orchestrator?` = `Application.static_seed/1` (verdade só no menor nó).
             No `BrokerServer`, o **orquestrador** faz `add_vnode` (start_cluster) de cada vnode; os **não-orquestradores**
             fazem `ReplicatedDSRSM.route_vnode/4` (novo — registra no ring + server de um membro, **sem** iniciar), de
             modo que exatamente um nó bootstrapa cada vnode (padrão RabbitMQ/`ra`). O `snapshot/1` ficou **tolerante**
             (vnode não-pronto → `Metadata` vazio, sem crash) e o `BrokerServer` **re-seeda o cache** dos clusters `ra`
             logo após o boot (janela de eleição) e periodicamente (`Broker.put_cache/2`), o que também cobre o
             multi-writer. Escolha **1B/seed estático** (vs 1A concorrente, arriscado no `ra`; vs orquestração-pelo-líder,
             que é a D-c-1d com fencing). Testado: `static_seed` (só o menor nó), `route_vnode` + `snapshot` tolerante
             (single-node), e `:multinode` — orquestrador inicia sobre 2 nós, não-orquestrador só roteia e lê/escreve
             cross-node. **Config:** `MALACHIMQ_LOG_VNODE_REPLICATION_FACTOR`.
           - ✅ **D-c-1d — `membership_leader` + reconcile loop.** A política de orquestração passa do seed
             estático para `Application.membership_leader/1` — verdade só no menor nó **vivo** (`MembershipServer.
             alive_members`, SWIM), então o papel **faz failover** quando o líder cai (tolerante: se a membership
             não responde → não-líder, nunca dois). O bootstrap vira **reconcile** (controller-style, k8s): no
             boot **todo** nó só faz `route_vnode` (`build_replicated` sem `start`); o `BrokerServer` reconcilia
             (level-triggered, idempotente) logo após o boot e periodicamente — e **só no líder** — chamando
             `MetadataServer.start/2` nos vnodes cujo cluster ainda não está pronto (`MetadataServer.ready?/1`,
             novo). O **fencing** é o nome do cluster `ra` (um segundo `start` do mesmo vnode falha sem
             duplicar — validado empiricamente); o *lease* (jeito k8s literal) fica para o **1C**, onde o líder
             passa a fazer trabalho **contínuo** (retention/healing/rebalancing). `static_seed/1` permanece como
             alternativa (testada). Testado: `membership_leader` (menor-vivo, tolerância); integração — o líder
             bootstrapa os vnodes via reconcile e um `create_topic` commita. Ver seção 8 (referências k8s/riak_core).
      2. **Detecção/reação a liderança Raft por vnode** — um supervisor que sobe/derruba coordinators
         conforme a liderança muda (via eventos do `ra`), tolerando oscilação e split-brain momentâneo.
    Sequência: D-b ✅ → **D-c-1 placement de vnodes** ✅ → **1C-a coordinators só-no-líder** ✅ →
    **1C-b-i detecção de liderança Raft por vnode** ✅ → **1C-b-ii-α coordinator apontado a um vnode** ✅
      → **1C-b-ii-β supervisor/manager per-vnode-leader** ✅. **1C-b completo.**

    - ✅ **1C-a — coordinators só-no-líder (sem lease).** `RetentionCoordinator` e `HealCoordinator`
      ganham o seam `:leader?` (`(-> boolean())`, default sempre); a cada tick, só varrem/curam se
      `leader?()` — senão o tick corre mas pula. O `Application` injeta `membership_leader(Malachi.
      LogMembership)` (reusa D-c-1d) nos dois quando clustered (`coordinator_leader?/1`; single-node =
      sempre age). Elimina a **redundância** (N nós faziam o mesmo trabalho) mantendo o modelo
      level-triggered. **Sem lease:** o trabalho é idempotente + roteado ao `ra` (serial), então dois
      coordinators transitórios (convergência SWIM) só refazem trabalho, não corrompem — o mesmo
      raciocínio do bootstrap. `run_now/1`/`heal_now/1` ignoram o gate (triggers manuais). Testado:
      não-líder pula o tick; o trigger manual age mesmo assim.
    - ✅ **1C-b-i — detecção de liderança Raft por vnode (núcleo puro).** `MetadataServer.leader?/1`
      espelha `ready?/1`: lê o líder que `:ra.members` reporta (qualquer membro alcançável responde) e
      é verdade só se ele for o **próprio** `server_id` — passe o server **local** (`{vnode_id, node()}`)
      para perguntar "este nó lidera este vnode?". Cluster não-formado/inalcançável → false (nunca
      assume liderança). `Application.leading_vnodes/3` é o seletor puro: dado o placement
      (`[{vnode_id, token, nodes}]` do bootstrap), o nó local e o predicado `leader?` (default
      `MetadataServer.leader?/1`), retorna os vnodes que o nó **hospeda** (placement o inclui) **e**
      **lidera** — curto-circuitando `leader?` para vnodes não-hospedados. É onde 1C-b-ii vai rodar os
      coordinators, um por vnode, no líder Raft dele (o NorthGuard literal, distribuindo a carga vs o
      líder único de membership do 1C-a). Testado: `leader?` no líder single-node (ra real) + não-formado
      → false; `leading_vnodes` filtra host×lidera, preserva ordem, não consulta liderança de não-hospedado.
    - ✅ **1C-b-ii-α — coordinator apontado a um vnode.** `Application.vnode_metadata_source/1` é um
      `metadata_source` ligado a **um** vnode: lê a visão local do `Metadata` daquele vnode via
      `MetadataServer.query({vnode_id, node()}, & &1)` (consistent query ao ra do vnode), **tolerante**
      (vnode não-formado/inalcançável → `Metadata.new()`, sem crashar o coordinator). Como o tipo é o
      mesmo (`Metadata.t()`) e `expire_segment`/`apply_heal` já roteiam ao vnode dono por topic, o
      `RetentionCoordinator`/`HealCoordinator` **não mudam** — basta trocar o source (por-vnode em vez do
      merge global) e o gate (`MetadataServer.leader?({vnode_id, node()})`). Testado (ra real): source
      tolerante em vnode não-formado; lê só o shard do vnode; um `RetentionCoordinator` ligado a um vnode
      expira só os segments **daquele** vnode.
    - ✅ **1C-b-ii-β — supervisor/manager per-vnode-leader.** `Malachi.Cluster.VnodeCoordinatorManager`
      (GenServer genérico, testável por seams `leading`/`spawn`/`stop`) reconcilia por **polling
      level-triggered** (logo após o boot via `handle_continue`, depois a cada `:vnode_reconcile_interval_ms`,
      default 5s): compara os vnodes que este nó lidera agora (`leading_vnodes/3` sobre `MetadataServer.
      leader?`) com os que já roda, **sobe** um par retention+heal para os recém-liderados e **derruba** os
      que deixou de liderar. Cada par vive sob um **supervisor por-vnode** (`:one_for_one`) num
      `DynamicSupervisor` (`Malachi.LogVnodeCoordinatorSupervisor`), para que um coordinator que crashe
      reinicie sem o manager perder o handle. No `Application`, `coordinator_children/2` **substitui** os
      coordinators únicos do 1C-a pelo par supervisor+manager **quando sharded** (`vnode_placement/2` ≠
      nil, extraído e reusado por `metadata_opts`); single-node / cluster de 1 vnode seguem no 1C-a.
      Cada coordinator mantém o gate `MetadataServer.leader?({vnode_id, node()})` como **defesa em
      profundidade** (se o manager atrasar numa oscilação, o coordinator não age após perder a liderança).
      **Idempotente + sem lease** (mesmo raciocínio do 1C-a): um flap transitório só refaz trabalho
      roteado ao `ra`, não corrompe. Testado: reconcile por seams (sobe/derruba/idempotente/esvazia) +
      integração (ra real, single-node) — o manager sobe um `RetentionCoordinator` real por vnode liderado
      e cada um expira só os segments do **seu** vnode. O **lease sobre `ra`** fica para quando o
      coordinator ganhar trabalho **não-idempotente** (rebalancing com movimento de dados).

    (A alternativa **1B** — coordinators iterando por-vnode mas ainda centralizados — evita materializar
    o merge, mas é um meio-termo sem gargalo medido; preterida em favor de ir direto ao placement.)

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

---

## 8. Referências de design externas: riak_core e Kubernetes

Analisamos três repositórios **riak_core** (a biblioteca de consistent hashing / vnodes / membership /
handoff do Riak) e o **Kubernetes**, para aprender como sistemas maduros resolvem os problemas que a
Fase 1 (distribuição) enfrenta: bootstrap distribuído, leader election com fencing, placement com
tolerância a falha, e reconcile loops.

**Enquadramento comum.** Os três convergem no mesmo padrão para coordenar shards: um **coordenador único
eleito**, com **fencing via consenso**. RabbitMQ (mesma lib `ra` que usamos) fencia pelo **nome do
cluster**; riak_core usa um *claimant* (fencing **fraco**, gossip); k8s usa um *Lease* sobre etcd
(fencing **forte**, CAS linearizável). **Veredito geral: referência de design, não dependência** — o
riak_core é **AP** (gossip + vector clocks; consenso forte só no `riak_ensemble`, externo) e o k8s
centraliza o metadata num **único** cluster **etcd** (Raft, não-sharded). O malachi é **CP e sharded**
(um cluster `ra` por vnode), então nenhum serve como dependência direta, mas os **algoritmos e padrões
são portáveis** — trocando "gossip" por "Raft" e preservando determinismo.

### 8.1 riak_core (AP) — referência: OpenRiak (`~/riak_core`, Apache 2.0, fork ativo)

- **Onde o malachi já está à frente (por ser CP):** metadata em Raft (> gossip); SWIM com suspicion
  (> gossip simples); failover automático; retention; atributos rack/DC. *Não regredir ao portar.*
- **`riak_core_claimant` → D-c-1d + rebalancing:** o modelo **staged → planned → committed** (o *plan*
  computa o ring novo sem mudar estado; o *commit* valida que nada divergiu) + eleição **lexicográfica**
  (menor node = o nosso `static_seed`). O fencing do claimant é **fraco** (gossip + vclock, split-brain
  possível) — o que **valida** a decisão do malachi de fenciar o bootstrap pelo **nome** do cluster `ra`.
- **`riak_core_claim_binring` V4 → upgrade do `place_vnodes`:** garante réplicas em nós **e** *locations*
  (rack/DC) distintos (`target_n_val`), balanceamento uniforme (k ou k+1 por nó) e rebalanceamento com
  **movimento mínimo** (`update()` antes de `solve()`). Doc bem comentada: `~/riak_core/docs/claim-version4.md`.
- **Ring versioning + ciclo de claim → re-clustering dinâmico** (add/remove nós) — **feito** no rebalancing
  R1→R3 (diff do placement vivo + `apply_plan` sob o lease); o gatilho segue **manual** (ver 8.4).
- **Ignorar:** gossip do ring, vector clocks (o Raft já dá ordem/consenso), `node_watcher` (o SWIM
  resolve), preflist-sobre-vnodes (desvio intencional — o malachi sharda **metadata por topic**, não
  distribui keys de dados sobre o ring).

### 8.2 Kubernetes (CP via etcd único)

- **Leader election + Lease → D-c-1d e, sobretudo, 1C (coordinators no líder):** o "triângulo"
  `LeaseDuration > RenewDeadline > RetryPeriod` + **CAS linearizável** (etcd; o nosso `ra` dá o mesmo) +
  **desistir proativo** ao não conseguir renovar (`OnStoppedLeading` — evita split-brain) + **relógio
  local** (tolera clock skew; premissa: NTP, drift ≤ ~`lease/10`). Traduz para um **lease armazenado num
  cluster `ra`** (comando CAS versionado). **Nuance-chave:** o fencing-por-nome do `ra` **basta para o
  bootstrap** (start único, auto-fencido); o **lease** só é necessário para o **trabalho contínuo** do
  líder — retention/healing/rebalancing (o 1C). Arquivos: `client-go/tools/leaderelection/`
  (`leaderelection.go`, `resourcelock/leaselock.go`), `api/coordination/v1/types.go`.
- **Controller/reconcile → coordinators + reconcile de bootstrap:** **level-triggered** (reconciliar o
  estado completo *desejado × atual* — os coordinators do malachi já fazem isso); **idempotência**;
  **workqueue** com dedupe + rate-limit + retry/backoff; **expectations** (rastrear operações em voo com
  TTL, para não re-agir cedo demais); **só-o-líder-age**; **observabilidade** (healthz de convergência).
  Referência: `pkg/controller/replicaset/replica_set.go`, `client-go/util/workqueue`.
- **Scheduler / PodTopologySpread → `place_vnodes`:** `topologyKey` (= o nosso `spread_by`/atributos),
  **`maxSkew`** (desbalanceamento máximo entre racks/zonas), **`minDomains`** (domínios distintos
  mínimos), **`whenUnsatisfiable`** (hard `DoNotSchedule` vs soft `ScheduleAnyway`), e pipeline
  **Filter → Score**. **Ressalva crítica:** o k8s **randomiza** o tie-break; o `place_vnodes` deve
  permanecer **determinístico** (raft-safe: toda réplica computa o mesmo placement). Portar as *ideias*
  (maxSkew / minDomains / hard-soft) sobre funções puras, sem randomização; e adotar *sticky preference*
  no `heal()` (preferir réplicas sobreviventes → menos churn). Referência:
  `pkg/scheduler/framework/plugins/podtopologyspread/`.
- **Trade-off registrado:** o etcd único é simples, mas é o **gargalo de escala** do k8s (~5000 nodes por
  cluster). O sharding do malachi paga complexidade (bootstrap / leader / fencing) justamente para
  **escalar além de um quórum** — a motivação da fatia D.

### 8.3 Síntese — como isso informou as fatias (o histórico de execução)

> Nota: as fatias abaixo (D-c-1d, 1C, place_vnodes, rebalancing) **estão feitas** — este bloco é o
> histórico. O resumo do que foi adotado × desviado está em **8.4**.

- **D-c-1d (`membership_leader`):** eleição pelo menor nó **vivo** (SWIM) + fencing por nome do `ra` no
  bootstrap; reconcile loop **level-triggered / idempotente** (padrão *controller* do k8s).
- **1C (coordinators no líder):** aqui entra o **Lease sobre `ra`** (k8s) para fenciar o trabalho
  **contínuo**, e o modelo **staged / planned / committed** (riak_core claimant) para mudanças de ring.
- **Upgrade do `place_vnodes`:** `target_n_val` / location-awareness (riak_core binring) + `maxSkew` /
  `minDomains` / hard-soft (k8s topology spread), **mantendo o determinismo**.
  - ✅ **A1 — rack-spread (feito).** `place_vnodes/4` ganha `place_opts`, repassado a `Placement.place/4`;
    com `[spread: {attribute_key, node_attributes}]` as R réplicas de cada vnode caem em **racks/zonas
    distintos** (`target_n_val`/`minDomains`), então perder um rack inteiro não leva a maioria das réplicas
    de um vnode. Reusa o `spread` já existente (round-robin por valor de atributo, do C3a). A topologia é
    **config estática** (`log_topology`, `"node=rack,..."`, `parse_topology/1`), idêntica em todo nó →
    placement **determinístico**. `Application.metadata_opts` liga o spread via `vnode_place_opts/0`
    (`:log_spread_by` + `:log_topology`). Testado: cada vnode com R=2 abrange os 2 racks; determinismo.
  - ✅ **A2 — balanceamento global de carga (`maxSkew`).** `Placement.place_balanced/4` coloca o
    **conjunto inteiro** de vnodes com **carga limitada** (visão global, não por-vnode): cada vnode ainda
    rankeia por HRW, mas um nó no teto (`ceil(total/nós) + max_skew - 1`) é **pulado** para o próximo, de
    modo que nenhum nó fica sobrecarregado. Determinístico (ranking + ordem + contadores iguais em todo
    nó). O teto é **best-effort** — o RF **nunca** é sacrificado por balanceamento, então um vnode que não
    alcançaria `min(rf, nós)` réplicas distintas pega o nó menos-carregado mesmo acima do teto (possível
    com `rf > 1`, onde o greedy por-vnode não empacota perfeito); com `rf = 1` o teto é **rígido**. Com
    `max_skew` grande degrada a HRW puro (movimento mínimo). É **standalone** (não combina com o
    `:spread` do A1 — mutuamente exclusivos; A2 tem precedência). `place_vnodes/4` usa `place_balanced`
    quando `[max_skew: n]`; `vnode_place_opts` liga via `:log_max_skew` (`MALACHIMQ_LOG_MAX_SKEW`).
    Testado: HRW puro empilha 6 de 9 vnodes num nó, balanceado espalha 3/3/3; property do teto rígido
    (rf=1) e de que todo vnode recebe `min(rf,nós)` distintas; determinismo; degradação a HRW com folga.
    Resolve **carga**, não perda de dados (a segurança de rack é o A1).
- **Rebalancing dinâmico** — quando a membership muda (nó entra/sai), redistribuir os vnodes ao vivo.
  Escopo: **control plane** (os membros dos clusters `ra` de cada vnode); adicionar/remover membro
  (`:ra.add_member`/`:ra.remove_member`) faz o **próprio `ra` transferir o estado** (Raft log/snapshot),
  então não movemos dados de metadata à mão; o **data plane** (segments) já é coberto pelo *healing*
  (1C-b). Modelo **manual** *staged → planned → committed* (riak_core; gatilho automático fica para
  depois, por cima do mesmo motor). Decomposto em:
  - ✅ **R1 — `desired_placement` (núcleo puro).** `Application.desired_placement/5` recomputa o placement
    desejado sobre um conjunto de nós arbitrário (a membership **viva**, vs a config estática `:log_nodes`):
    compõe `sharded_vnodes/2` (vnodes lógicos fixos) + `place_vnodes/4` (HRW). Determinístico e
    **movimento mínimo** — um vnode só muda se **adotar** um nó que entrou ou **detinha** um que saiu; o
    resto fica posto. Testado: determinismo; ao **adicionar** um nó, um vnode só muda se adota o novo nó
    (e algum adota); ao **remover**, só muda quem o detinha (e ele some do placement); clamp a `min(rf, |nós|)`.
  - ✅ **R2 — plano de rebalanceamento (núcleo puro).** `Application.rebalance_plan/2` faz o diff do
    placement **atual** × `desired_placement` (R1) por vnode: para cada vnode cujo conjunto de nós difere,
    devolve `%{vnode_id:, add:, remove:}` (nós a entrar / a sair do cluster `ra`); vnodes já corretos são
    omitidos (plano vazio = nada a fazer). *Staged/planned* — computa sem aplicar. A ordem segura é
    **add-before-remove** (o R3 adiciona antes de remover, então um vnode nunca cai abaixo do quórum no
    meio da mudança; com RF constante, `add` e `remove` têm o mesmo tamanho). Assume o **mesmo conjunto de
    vnode ids** em atual e desejado (mudar a contagem é re-sharding, fora de escopo). Determinístico
    (segue a ordem do desejado). Testado: vazio quando nada muda; add/remove por vnode alterado (omite os
    iguais); num *join* o RF fica constante (add/remove equilibrados → nunca abaixo do quórum); num
    *leave* só os vnodes que detinham o nó que saiu entram no plano e nenhum re-adiciona o nó removido.
  - **R0 — Lease sobre `ra`** (fencing forte, k8s): pré-requisito de R3 (o movimento de vnodes é
    **não-idempotente** — é aqui que o lease finalmente entra, como antecipado no 1C-b).
    - ✅ **R0-a — máquina de estado do lease (núcleo puro + `ra`).** `Malachi.Cluster.Lease` é o estado
      puro (`holder`, `fence`, `renew_at`, `duration_ms`): `acquire_or_renew` concede se **livre**, **já é
      o holder** (renovação) ou **expirado** (`now >= renew_at + duration_ms`), senão `{:error, {:held,
      holder}}`; `release` é idempotente. O **fencing token** (`fence`) é monotônico e sobe **só quando o
      holder muda** (renovação mantém) — o holder o carrega para o trabalho que fencia, e uma escrita de
      ex-holder com token obsoleto pode ser rejeitada (a proteção contra dois chefes). O tempo (`now`) é
      **injetado**, nunca lido dentro do `apply` (seria não-determinístico e quebraria o Raft):
      `LeaseMachine` (`:ra_machine`) alimenta o `meta.system_time` do `ra` (relógio do **líder**,
      carimbado uma vez e replicado no log), então um único relógio decide a expiração — sem o skew
      entre nós que um tempo vindo do cliente carregaria. `LeaseServer` espelha o `MetadataServer` sobre
      um cluster `ra` **dedicado** (isolado do metadata). Testado: `Lease` puro exaustivo (aquisição/
      renovação mantém fence/roubo na expiração incrementa fence/fronteira exata do deadline/release
      idempotente com token obsoleto) + integração `ra` real (acquire/renew/held/release/durável a restart).
    - ✅ **R0-b — `LeaseHolder` (o client).** GenServer que roda o triângulo de timers `duração >
      renew_deadline_ms > retry_period_ms`: a cada `retry_period` chama o seam `renew` (acquire-or-renew);
      um *follower* que adquire vira *leader* e chama `on_acquired(fence)`; um *leader* que renova segue
      líder (marca o instante do renew no **relógio local**); se lhe dizem que o lease está com **outro**
      larga na hora (`on_lost`); se **não alcança** o lease, segue tentando até passar `renew_deadline_ms`
      desde o último renew bem-sucedido e então **larga proativo** (`on_lost`, o *OnStoppedLeading* do k8s
      — desiste antes de o lease poder expirar/ser roubado, para nunca haver dois líderes). Um salto do
      **fencing token** durante a liderança (gap: perdeu e reganhou) dispara `on_lost` seguido de
      `on_acquired` sob o novo token. No shutdown normal, um líder **libera** o lease (failover sem
      esperar expiração). Tudo por **seams injetados** (`renew`/`release`/`clock`/callbacks), então a
      lógica de tempo é testada sem `ra`, controlando o relógio. Testado: adquire→líder; renova sem
      re-`on_acquired`; segura até o deadline e larga; largada imediata quando held-por-outro; troca de
      token → lost+acquired; release no shutdown do líder (e não do follower). **R0 completo.**
  - **R3 — execução** (*committed*): aplica o plano por vnode via `ra`, **sob o lease**. Escopo: control
    plane (o `ra` transfere o estado ao adicionar membro); o data plane fica com o *healing*.
    - ✅ **R3-a — executor de uma mudança (núcleo com seams).** `Malachi.Cluster.Rebalance`: `apply_change/3`
      aplica cada `add` **antes** de cada `remove` (add-before-remove, para o vnode nunca cair abaixo do
      quórum) via seams `add_member`/`remove_member` (`(vnode_id, node -> :ok | {:error, _})`); **idempotente**
      (add de quem já é membro / remove de quem já saiu = `:ok`, então um commit interrompido é
      re-executável); **fail-fast** (um `add` que falha **não** tenta os removes — protege o quórum).
      `apply_plan/4` aplica o plano mudança-a-mudança, fail-fast entre vnodes (para no 1º erro, devolve
      `{:error, {aplicados, falha}}`), e **revalida `leader?` antes de cada mudança** (para com
      `:lost_leadership` se o holder soltou o lease no meio). Testado (seams que gravam a ordem): add
      antes de remove; add que falha não remove; erro no remove reportado; idempotência; plano completo;
      fail-fast entre vnodes; parada por perda de liderança.
    - **R3-b — coordenador plan/commit + ops `ra`/wiring.**
      - ✅ **R3-b-i — plano do estado vivo (núcleo com seams).** `Application.readable_placement/2` monta o
        placement **atual** a partir das memberships `ra` dos vnodes via o seam `members_of`
        (`vnode_id -> {:ok, nodes} | {:error, _}`), **omitindo** vnodes ilegíveis (conservador: nunca
        planejar um vnode que não conseguimos ver). `Application.live_rebalance_plan/5` é o *plan*: faz o
        diff do atual (legível) contra o `place_vnodes` **desejado** sobre os nós **vivos**, só para os
        vnodes legíveis, e devolve o plano (`rebalance_plan/2`) que alimenta `Rebalance.apply_plan/4` (o
        *commit*, sob o lease). Fica junto de R1/R2 no `Application` (evita ciclo com `Rebalance`, que só
        executa). Testado: `readable_placement` omite ilegível; `live_rebalance_plan` = diff atual×desejado
        sobre vivos; vazio quando já casa; nunca planeja vnode ilegível.
      - ✅ **R3-b-ii — ops `ra` reais + coordenador (motor).** `Rebalance.ra_add_member/3` e
        `ra_remove_member/3` são os seams reais do `apply_plan`: **add** = `add_member` (anuncia o membro —
        ele nem precisa estar rodando) **depois** `start_server` no nó via `:erpc` (a ordem que a doc do
        `ra` prescreve; o líder então replica log/snapshot ao novo membro); **remove** = `remove_member`
        depois `stop_server`. **Idempotentes** (`already_member`/`not_member`/`already_started` aninhado →
        `:ok`) e com **retry** em `:cluster_change_not_permitted` — o `ra` só permite **uma** mudança de
        membership por vez, então o `add`-then-`remove` de um mesmo change (e ops repetidas) esperam a
        anterior assentar. `RebalanceCoordinator` (GenServer, seams `plan_fun`/`add_member`/`remove_member`/
        `leader?`) expõe `plan/1` (calcula, não aplica) e `commit/1` (**recusa `:not_leader`** se não for o
        holder do lease; senão `apply_plan` fail-fast, passando o mesmo `leader?` para parar se o lease cair
        no meio). Commit **sempre manual**. Testado: coordenador por seams (plan/commit/recusa/fail-fast) +
        **`:multinode`** real — `ra_add_member` cresce um vnode para um novo nó e o `ra` transfere o estado,
        `ra_remove_member` encolhe, ambos idempotentes.
      - ✅ **R3-b-iii — wiring (\"ligar na tomada\").** Quando **sharded**, `Application.log_children`
        adiciona `rebalance_children`: bootstrapa o `LeaseServer` (cluster `ra` dedicado `Malachi.LogLease`,
        **auto-fencido** no boot — todo nó chama, um forma) e sobe o `LeaseHolder` (`Malachi.LogLeaseHolder`,
        triângulo default 15s/10s/2s via `lease_duration_ms`/`lease_renew_deadline_ms`/`lease_retry_period_ms`,
        `renew`/`release` reais sobre o `LeaseServer`) e o `RebalanceCoordinator` (`Malachi.LogRebalanceCoordinator`)
        com os seams reais: `plan_fun`=`live_rebalance_plan`, `add_member`/`remove_member`=`Rebalance.ra_*`
        resolvendo os membros via `try_members/2` (tenta cada nó como ponto de entrada — o holder pode não
        hospedar o vnode; qualquer membro roteia ao líder), `leader?`=`LeaseHolder.leader?`. Adicionei
        `LeaseHolder.leader?/1` (lê o papel sem forçar tick). O **commit segue manual** (o operador chama
        `RebalanceCoordinator.plan/1`/`commit/1`); o `LeaseHolder` só mantém a eleição rodando (k8s). O
        caminho **não-sharded é inalterado**. Testado: `leader?/1` e `try_members/2` isolados + suíte
        completa (1048 testes) verde = boot não regride; comportamento das ops apoiado no `:multinode` de
        R3-b-ii. **Rebalancing dinâmico completo — a Fase 1 (distribuição) fecha aqui.**
      - ✅ **Reconcile do lease (endurecimento).** O bootstrap `auto-fencido` acima forma o cluster do
        lease só com a **maioria** (Raft); um nó que estava down quando o cluster formou fica membro da
        **config** (o `start_cluster` inicial lista todos os nós) mas sem servidor rodando, reduzindo a
        tolerância a falha do lease. `LeaseServer.reconcile/2` faz **self-join** — best-effort e idempotente:
        (re)tenta formar o cluster (`start/2`, auto-fencido) e iniciar o servidor **local** (`:ra.start_server`),
        que se re-junta ao cluster existente (já é membro da config) e o `ra` replica o estado do lease a
        ele. `Malachi.Cluster.LeaseReconciler` (GenServer genérico, seam `:reconcile`) o chama após o boot
        e a cada `lease_reconcile_interval_ms` (default 30s), *level-triggered* — mantém o `LeaseHolder`
        livre de `ra`/membership. Subido no `rebalance_children` (primeiro, antes do holder). Testado:
        reconcile bootstrapa quando não iniciado + é no-op idempotente num cluster formado (não perturba o
        lease); o reconciler reconcilia no boot e sob demanda.
    - ✅ **Descoberta dinâmica de nós (libcluster) — connectivity-only.** Fecha a lacuna de operabilidade: a
      descoberta de peers era **estática** (`MALACHIMQ_LOG_NODES` + `Node.connect` manual / hostnames fixos).
      Dep `{:libcluster, "~> 3.5"}` + um `Cluster.Supervisor` **opcional** na árvore (só quando
      `MALACHIMQ_CLUSTER_STRATEGY` está setado; ausente = single-node, sem exigir distribuição — default
      intacto). Decisão (**1A**): **connectivity-only** — libcluster só descobre+conecta nós (distribuição
      Erlang); SWIM e o `ra` seguem usando o `log_nodes` para o *member set* inicial, e a mudança de
      membership do `ra` continua pelo **R3 (rebalancing sob lease)** já feito — não duplica, de forma menos
      segura, a formação Raft. Estratégias (**2A**): `gossip` (UDP multicast, dev/LAN), `kubernetes`
      (descobre pods via API; `selector`+`node_basename` obrigatórios), `epmd` (lista estática reusando
      `log_nodes`). Módulo **puro** `Malachi.Cluster.Topology.build/1` mapeia config→topologies (fail-fast:
      raise em campo obrigatório faltante), unit-testável sem abrir socket multicast/k8s. Env parseado no
      `runtime.exs` (`log_nodes` extraído p/ binding, reusado no `epmd`). Testado: `build/1` por estratégia
      (defaults, campos obrigatórios, nils omitidos, unknown raise) — 12 testes; smoke de boot real (default
      → sem `ClusterSupervisor` e sem distribuição; `gossip` + `--sname` → `ClusterSupervisor` vivo). Suíte
      completa 715 testes 0 falhas (boot não regride); credo/dialyzer limpos. README com a seção de node
      discovery. **Fatia de operabilidade multi-nó fechada.**
    - ✅ **Hardening de placement — garantia de domínios de falha (`min_domains`/`policy`).** O `Placement`
      já fazia spread rack/DC + `max_skew`, mas o `:spread` é **best-effort**: com menos domínios que `rf`,
      ou atributos faltando, as réplicas concentravam **silenciosamente** — um furo de HA (3 réplicas no
      mesmo rack sobrevivem a zero falhas de rack). Decisão **1A**: `:hard` **falha rápido na colocação
      inicial**; heal segue best-effort (durabilidade primeiro) + reporta. **Core (puro)**: `place/4` ganha
      `:min_domains` (nº mínimo de valores distintos do atributo que o replica set deve cobrir; sem
      `:spread`, brokers distintos) + `:policy` (`:soft` default = comportamento atual; `:hard` retorna
      `{:error, {:insufficient_domains, coberto, exigido}}`). Broker sem atributo cai no domínio `nil` único
      (conservador: não conta como domínio extra). Novo `domain_violations/4` reporta segmentos cujo replica
      set cobre < `min_domains` domínios (alerta/observabilidade). **Fiação**: broker (`min_domains`/
      `placement_policy` no struct/open; `place_opts` injeta; `open_segment` trata `{:error, ...}` → produce
      aborta limpo, `register_segment` extraído), broker_server (threading), application (`data_plane_opts`
      lê `log_min_domains`/`log_placement_policy`), config (`MALACHIMQ_LOG_MIN_DOMAINS`/
      `MALACHIMQ_LOG_PLACEMENT_POLICY`). **Fix relacionado (Issue 2)**: o heal era **rack-blind** —
      `self_healing` chamava `place/3` sem `:spread`; agora `HealCoordinator` resolve o spread por pass (via
      `heal_spread/0`, atributos vivos) e o `self_healing` forwarda **só `:spread`** (strip de
      `min_domains`/`policy` — heal nunca hard-falha). Testado: `place/4` min_domains/policy (soft/hard,
      met/unmet, sem-spread, nil-domain) + `domain_violations/4` (5+3); broker hard-fail e2e (produce aborta
      com 2 racks/min_domains 3; soft coloca; hard passa com min_domains 2 — 3); heal rack-aware forwardando
      `:spread` (1). Suíte 727 testes 0 falhas; credo/dialyzer limpos. README com os env vars.
      - ✅ **Surfacing de `domain_violations` (métrica + painel).** Fecha o loop da metade "reportar" do 1A: o
        `domain_violations/4` era uma função pura que **nada chamava**, então com política **soft** o operador
        ficava cego para a degradação de HA. `Broker.domain_violations/1` (puro) computa, do próprio broker
        (merged metadata + `spread_by` + `broker_attributes` vivos + `min_domains`), as violações **por topic**
        (`%{topic => count}` via `Enum.frequencies_by(&topic_of_segment/1)`; `%{}` se spread/min_domains não
        configurados); `BrokerServer.domain_violations/1` expõe via call. O `dashboard` anexa o count a cada
        topic no `topics_overview` (default 0), o `Prometheus.export` emite o gauge por-topic
        `malachi_domain_violations` (`Map.get(.., 0)` defensivo), e o painel mostra um badge `⚠ N HA` **só
        quando > 0** (alto sinal, sem clutter). Testado: `Broker.domain_violations` (soft abaixo do alvo → 1;
        no alvo → vazio; não configurado → vazio) + gauge do Prometheus (emite por-topic, default 0 na
        ausência da chave) — 4. Suíte 731 testes 0 falhas; credo/dialyzer limpos; badge JS validado. README
        com o gauge.
    - ✅ **Exemplo de deploy Kubernetes (amarra libcluster + placement num deploy real).** `deploy/kubernetes/`
      — um manifest (`malachi.yaml`, 8 docs) de um **cluster CP de 3 nós** com placement rack (zona) aware,
      + README explicando o racional. Decisões: **1A** descoberta por **epmd (lista estática de FQDNs)** —
      um StatefulSet CP/Raft tem identidades **estáveis** (idiomático, e os node names casam com o
      `RELEASE_NODE` — determinístico, o que importa já que não há cluster real p/ testar); **2A** rack-aware
      **de verdade** via init container. Peças: **StatefulSet** (identidade/DNS/PV estáveis p/ o `ra`;
      `podManagementPolicy: Parallel` forma o quórum junto), **headless Service** (`publishNotReadyAddresses`
      p/ os peers se resolverem durante a formação), **client Service** (4040/4041), **PDB** `minAvailable: 2`
      (maioria Raft em drains), **ClusterRole** least-privilege (`get nodes`) + SA/binding p/ o init. Node name
      distribuído via `RELEASE_NODE=malachi@$(POD_NAME).malachi-headless.$(POD_NAMESPACE)...` +
      `RELEASE_DISTRIBUTION=name` + `ERL_AFLAGS` fixando a porta de dist; peer set do `ra` = os 3 FQDNs em
      `MALACHIMQ_LOG_NODES`; `MALACHIMQ_CLUSTER_STRATEGY=epmd` reusa a lista. Rack-awareness:
      `topologySpreadConstraints` por zona + init container (`kubectl get node`) escreve a zona num emptyDir
      que o main container dobra em `MALACHIMQ_LOG_ATTRIBUTES=zone=<z>`, com `LOG_SPREAD_BY=zone`/
      `MIN_DOMAINS=2`/`PLACEMENT_POLICY=soft` (violações viram o gauge do slice anterior; nó sem label →
      fallback informativo). Probes `/health`+`/ready` (do O1). Sem mudança de código Elixir (node name via
      `RELEASE_*`; o `vm.args` já usa `inet_res`). Validado: YAML parseia (8 docs) + spot-check dos valores
      críticos (ordem do env com POD_NAME antes do RELEASE_NODE, refs `$(VAR)`, command colapsado). Doc da
      alternativa `kubernetes` (dinâmica, RBAC em pods) p/ deploys autoescaláveis. README principal aponta.
      (Não testável sem um cluster k8s real — config determinística por construção.)
    - ✅ **TLS na distribuição Erlang inter-nó (G3).** Fecha um gap de segurança de produção: metadata
      (`ra`) + replicação de dados trafegavam em **texto puro** entre nós (só o cookie autenticava).
      Decisão **1A**: **TLS mútuo** (`verify_peer` + `fail_if_no_peer_cert`) — CA compartilhada, cert por nó,
      cifra **e** autentica. Config de VM/release (não código Elixir): `rel/env.sh.eex` traduz
      `MALACHIMQ_DIST_TLS=true` em `-proto_dist inet_tls -ssl_dist_optfile $MALACHIMQ_DIST_TLS_OPTFILE`
      (via `ELIXIR_ERL_OPTIONS`), **fail-fast** se o optfile faltar/for ilegível; default off = texto puro
      atual intacto. Artefatos: `rel/dist_tls.conf.example` (template do ssl_dist optfile), helper de dev
      `scripts/generate-dist-certs.sh` (CA + cert de nó com EKU server/clientAuth + emite um optfile pronto),
      `.gitignore` de `priv/dist_cert/` (nunca commitar chaves). Fiado no exemplo k8s (Secret `malachi-dist-tls`
      com ca/node cert+key+optfile, volume readOnly em `/etc/malachi/dist`, 2 env). **Validado localmente de
      fato** (≠ k8s): 2 nós BEAM sobre TLS dist se pingam (`:pong`), e um nó **sem** TLS é **rejeitado** no
      handshake (`:pang` — prova que a TLS é imposta, não silenciosamente plaintext); os 3 caminhos do
      `env.sh.eex` (off/on/fail-fast); optfile é term Erlang válido (`:file.consult`); YAML k8s parseia (9
      docs). Sem mudança de código Elixir; README (seção inter-node TLS) + deploy README.
    - ✅ **Shutdown gracioso / rolling-upgrade (G4).** O `prep_stop` antigo **fechava tudo de imediato**
      (sem quiesce nem janela → in-flight cortado, e race com accepts novos durante o fechamento). Decisão
      **1A** (janela limitada — o modelo certo para um broker com **streaming**, onde drenar-até-0-conexões
      nunca converge). Novo `Malachi.Shutdown.graceful/1` orquestra 3 passos: **quiesce** (`terminate_child`
      do `TCPAcceptorPool` no root supervisor — para de aceitar e **não** reinicia; as conexões, que são
      `spawn` unlinked registradas no `ConnectionRegistry`, sobrevivem) → **drain** (sleep
      `shutdown_grace_ms`, default 5s, janela para in-flight terminar) → **close** (`close_all`). O lease já
      é liberado pelo `LeaseHolder.terminate` na teardown seguinte (failover rápido) e o `ra` persiste em
      disco (o pod volta e re-join como o mesmo membro). Passos são **seams** → a orquestração
      (ordem + janela) é unit-testável sem parar o app real. k8s: `terminationGracePeriodSeconds: 40` +
      `preStop` (`sleep 5` — kube-proxy tira o pod dos endpoints do Service **antes** do SIGTERM, então
      clientes param de ser roteados antes do drain). Config `MALACHIMQ_SHUTDOWN_GRACE_MS`. Testado:
      `graceful/1` roda quiesce→sleep(drain_ms)→close **em ordem**; pula o sleep com `drain_ms: 0`;
      default vem do config — 3. Suíte verde; credo/dialyzer limpos. README (env var) + k8s (grace/preStop).
    - ✅ **Consumer group coordination (G1 — épico, fatiado; concluído S1–S5 + Str-1/Str-2).** Antes um grupo era
      uma **posição única compartilhada** (todos os consumidores liam a mesma posição commitada — sem paralelismo).
      Alvo NorthGuard/Kafka **atingido**: cada **range** do topic atribuída a **exatamente um** membro do grupo,
      consumo paralelo, com rebalance no join/leave — **tudo server-internal e opaco** (o cliente nunca vê ranges).
      Achado que aterrou o design: o `commit_offset` fazia `Map.put` (substituía o mapa de offsets do
      `{group, topic}`) → virou **merge por-range** (S2). Fatiamento: **S1** núcleo de assignment (puro) · **S2**
      commit por-range · **S3** coordinator (membership + heartbeat/session + expõe assignment) · **S4** integração
      no servidor (fetch respeita a assignment, opaco) · **S5** protocolo wire + cliente · **Str-1/Str-2**
      member-scoping do streaming (push server-side + wire/cliente com heartbeat). **Escopo restante (fatia própria,
      não-G1): o coordinator é hoje um GenServer local único — o roteamento/replicação multi-nó da membership fica
      para uma fatia de wiring de cluster.**
      - ✅ **S1 — núcleo de assignment (puro).** `Malachi.Consumer.Assignment.assign(range_ids, members)`
        → `%{member => [range_id]}`, cada range sob **exatamente um** membro, **determinístico** (ranges
        ordenadas em ordem canônica → um coordinator replicado/failover computa o mesmo em todo nó). Decisão
        **1A (HRW sticky)**, mas **corrigida por medição empírica** (o memory de medir antes de decidir): a
        opção dizia "reusa `place_balanced`", porém o property test revelou que ele **não é fortemente
        sticky** (N pequeno: o rebalance-pro-cap move ranges de sobreviventes — 4 ranges/4 membros, remover
        1 moveu 2 de 3 sobreviventes), contrariando a prioridade *sticky*. Troquei para **HRW puro**
        (`Placement.place(range, members, 1)` por range → o membro top-HRW), que dá **min-reshuffle
        estrito**: um leave move **só** as ranges do que saiu (sobreviventes mantêm **todas**), um join move
        ranges **só** para o novo membro (existentes só perdem, nunca trocam entre si) — a stickiness que
        "HRW sticky" promete, com balanço **estatístico** (ótimo com muitas ranges, o caso NorthGuard). Ainda
        reusa `Placement.place` (o ranking HRW). Testado (property): partição (cada range 1×), determinismo
        sob shuffle, **sticky-on-leave** (sobreviventes mantêm tudo), **sticky-on-join** (inalterado ou o
        novo) + edges (sem membros → `%{}`, sem ranges → membros idle, dedup). Suíte 746 testes 0 falhas;
        credo/dialyzer limpos.
      - ✅ **S2 — commit por-range (merge).** O `Metadata.apply({:commit_offset, group, topic, offsets})`
        deixava de fazer `Map.put` (substituía o mapa inteiro de offsets do `{group, topic}`) e passa a
        **`Map.update` + `Map.merge`** — mescla os offsets recebidos por-range (last-commit-wins por range).
        Assim um membro de um grupo particionado commita **só as ranges que possui** sem apagar as posições
        das ranges de outros membros — o pré-requisito que o S1 apontou. Backward-compatible: um consumidor
        único que commita o mapa completo funciona igual (merge cobre tudo), e os testes existentes (que
        commitam uma range ou a mesma 2×) passam sem mudança. Caminho único (o `DSRSM` roteia por topic pro
        `Metadata.apply`; `merged_metadata` une topics disjuntos). Tradeoff registrado: o merge deixa keys
        **stale** quando uma range faz split/merge (o offset da range antiga persiste) — mas é **bounded**
        pelo keyspace (máx ~2^keyspace_bits range_ids históricos) e **inócuo** na leitura (o fetch só consome
        ranges atuais; ranges mortas são ignoradas); um prune (reusando o índice `topic_ranges`) fica como
        otimização futura, não S2. Testado: novo teste de merge (dois membros, um commita só sua range → a do
        outro é preservada) + os existentes (last-wins por range). Suíte 747 testes 0 falhas; credo/dialyzer
        limpos. **Próximo: S3 (coordinator: membership + heartbeat + expõe a assignment do S1).**
      - ✅ **S3 — coordinator de grupo (membership + heartbeat + assignment).** `Malachi.Consumer.GroupCoordinator`
        (GenServer) rastreia os membros por `{group, topic}` e atribui as ranges do topic via S1. API: `join`
        (adiciona membro → rebalance → devolve `{:ok, generation, ranges}`), `heartbeat` (renova a sessão,
        devolve a assignment atual + generation, ou `{:error, :unknown_member}` se foi evictado → re-join),
        `leave`, `assignment` (leitura sem renovar), `reconcile_now` (roda um tick sync — seam de teste).
        **Eager (decisão 1A)**: qualquer mudança de membership (join/leave/eviction) ou de ranges recomputa a
        assignment inteira e **bump da `generation`** (epoch à la Kafka) — o membro re-lê no heartbeat e vê a
        generation nova = reassumir; **level-triggered** (só bumpa se a assignment mudou de fato, então o tick
        é idempotente). Session-timeout: um membro silencioso por `session_ms` é **evictado** no reconcile
        (tick periódico), suas ranges reatribuídas; grupo sem membros é **dropado** (sem leak de estado).
        Só a **lógica** do coordinator, como instância única, com seams (`clock`/`ranges_fun`) → testável sem
        cluster; o **roteamento no cluster** (qual nó coordena qual grupo — Kafka hasheia group→broker, ou
        replicar a membership) fica para a fatia de wiring. Estado de membro é **soft** (restart → membros
        re-join). Testado: membro sozinho pega tudo (gen 1); dois membros particionam (disjunto/completo, gen
        avança); leave devolve as ranges; **eviction** por session-timeout + re-join obrigatório; grupo vazio
        dropado; mudança de ranges rebalanceia no reconcile; reconcile idempotente (sem mudança → mesma gen);
        heartbeat de membro desconhecido rejeitado — 8. credo/dialyzer limpos.
      - ⚠️ **Correção de rumo (fidelidade NorthGuard) antes do S4.** Revisão apontou que S1–S3 importaram o
        **modelo Kafka** (assignment de `range_ids` **visível ao cliente**). Isso **violaria** o princípio
        central do projeto (doc §B: *"contrato de cliente = jeito NorthGuard, NÃO Kafka: … cursor opaco …
        nunca vê partition/offset"*). Ranges são o equivalente NorthGuard de partitions → têm de ficar
        **escondidas**. O maquinário S1–S3 é **server-side e correto** (o coordinator computa a assignment
        internamente); o `[range_ids]` que ele devolve é **detalhe interno**, não vai ao wire. Plano de S4/S5
        **reajustado para opaco**: o servidor escopa o fetch à assignment do membro e devolve records +
        **cursor opaco**; o cliente **nunca** vê range_id. Membership **implícita via fetch** (o fetch é o
        heartbeat) + `leave` explícito depois.
      - ✅ **S4 — consumo particionado server-side (opaco, in-VM).** `GroupCoordinator.poll/4` é o entry-point
        do fetch: registra o membro se novo (rebalance) ou só renova a sessão se conhecido (sem rebalance) e
        devolve as ranges dele — então o membro fica vivo **buscando**, sem heartbeat separado. O caminho do
        `consume` ganhou um filtro **`ranges`** (`consume_ranges/5` + `selected_ranges/3`): `nil` = todas as
        ranges ativas (grupo inteiro / consumidor único, comportamento atual); uma lista = **só** essas,
        **interseccionadas com as ativas** (uma range atribuída que já fez split é pulada). Threadado por
        `BrokerServer.consume/6` + o `handle_call({:consume})` (6-tupla) + o waiter do long-poll (guarda
        `ranges`) — subscriber de streaming inalterado (usa o default `nil`). Novo `LogApi.fetch_member/7`:
        `poll` no coordinator → as ranges do membro → consume escopado das posições commitadas (S2), retorno
        = records + **cursor opaco** (o cliente nunca vê range_id; o `commit` avança só as ranges do membro).
        Backward-compat: `fetch_group` sem membro = grupo inteiro. Testado: `poll` (registra novo/heartbeat
        conhecido/re-registra evictado — 2); **integração e2e in-VM** (topic com 2 ranges via split, 2 membros
        pré-registrados buscam **disjunto e completo** — cada record por exatamente um membro; backward-compat
        do fetch_group) — 2. Suíte 759 testes 0 falhas; credo/dialyzer limpos.
      - ✅ **S5 — wire + cliente (opaco). Consumer group coordination completo.** Expõe o consumo
        particionado do S4 sobre o protocolo binário **sem vazar range_id**. **Wire**: `fetch_req` ganha um
        **member id** (`put_str`, após o group; nil = grupo inteiro / consumidor único, backward-compat) —
        precedência member (grupo, escopado) > cursor (paging do cliente) > group (resume); nova op
        `leave_group` (api_key 7, `topic/group/member`, ack vazio). O `tcp_protocol` despacha `fetch` com
        member → `LogApi.fetch_member` (retorno = `encode_fetch_resp`, records + **cursor opaco**, idêntico ao
        fetch normal — zero range_id no wire) e trata `leave_group` → `GroupCoordinator.leave`. **Wiring**: o
        `GroupCoordinator` sobe na árvore (`Malachi.LogGroupCoordinator`, `ranges_fun` = `active_range_ids` do
        `LogBroker`). **Cliente Node**: `wire.js`/`client.js` (member no `fetch` + `leaveGroup`), `consumer.js`
        modo **`--member`** (fetch escopado + commit + `leave` no exit; vários membros do mesmo `--group` com
        `--member` distintos = consumo paralelo). Achado do dialyzer: o `@type api_key :: 0..6` fazia inferir
        que o branch `leave_group` (7) era morto → atualizado p/ `0..7`. Testado: wire round-trip (member +
        leave_group), **e2e via TCP** (member fetch server-scoped + cursor opaco + records sem offset +
        leave_group ack) + suíte binária existente (backward-compat do fetch sem member); smoke Node real
        (produce → consumer `--member` consome tudo como membro único + `leave`; consumer sem member =
        backward-compat). Suíte 761 testes 0 falhas; credo/dialyzer/format limpos. README (tabela api_key +
        exemplo de membros paralelos). **G1 (consumer group coordination) concluído** (S1–S5); pendências
        anotadas: member-scoping do **streaming** (push) e prune de offsets stale (fatias futuras).
      - ✅ **Prune de offsets stale (dívida do S2).** O merge por-range do S2 deixava uma key **morta** por
        split (o offset da range-pai persistia no mapa do grupo). O `apply({:commit_offset, ...})` agora,
        após o merge, **pruna** os offsets para as ranges **ativas** do topic (`prune_offsets/3` +
        `active_range_id_set/2`, reusando o índice `topic_ranges` do V-idx + o filtro `state == :active`):
        uma range retirada por split/merge tem o offset **descartado** — é seguro porque os filhos ativos
        resumem de `:start` (o consume só lê ranges ativas; a semântica at-least-once de cross-epoch não
        muda, só some a key morta). **Pulado quando o topic não está roteado** (offset commitado antes do
        `create_topic` — `topic_ranges` sem a entrada → mantém como está), preservando o comportamento
        pré-routing dos testes existentes. Bounda o mapa de offsets ao nº de ranges **ativas** (antes crescia
        ~2^keyspace_bits com splits). Testado: split sela o root → o offset do root é prunado no commit
        seguinte (fica só o do filho); os testes de merge/pre-routing do S2 seguem verdes (sem topic → sem
        prune). Suíte 762 testes 0 falhas; credo/dialyzer/format limpos.
      - ✅ **Streaming member-scoping — Str-1 (server-side, opaco).** Leva o consumo paralelo por grupo ao
        push/subscribe (antes whole-group). **Restrição arquitetural** que ditou o design: o
        `push_subscriber` roda **dentro** do handle_call do broker, mas o `ranges_fun` do coordinator
        **chama de volta** o broker (`active_range_ids`) → se o broker chamasse o coordinator sincronamente,
        deadlock (cada GenServer esperando o outro). Regra: **o broker nunca chama o coordinator.** Design:
        toda coordenação no **`LogApi`** — `subscribe_member/7` faz `poll` (registra + ranges) e passa
        `member`/`ranges`/`coordinator` ao `BrokerServer.subscribe` (via `group_opts`); o subscriber
        **armazena** as ranges e o `push_subscriber` escopa o consume com elas (`consume_ranges/5`);
        `stream_ack_member/7` re-poll (heartbeat + ranges frescas) → o broker **atualiza** as ranges do
        subscriber no ack (pega rebalance). **Liveness**: o `:DOWN` do broker dispara um **`Task` assíncrono**
        que chama `coordinator.leave` (async → não bloqueia o broker → sem deadlock) para rebalance rápido na
        desconexão; membro idle fica vivo por ack periódico do cliente (Str-2). Positions escopadas por
        `Map.take` no subscribe (como no `fetch_member`); **opaco** (o push segue `{:log_records, records,
        cursor}` — zero range_id). Testado in-VM: 2 membros pré-registrados recebem push **disjunto e
        completo** (topic com 2 ranges via split); processo de um membro morrendo → **leave** async → o
        membro some do coordinator. Suíte 764 testes 0 falhas; credo/dialyzer/format limpos. **Próximo: Str-2
        (wire: member no subscribe/stream_ack + cliente Node subscriber com member + heartbeat).**
      - ✅ **Streaming member-scoping — Str-2 (wire + cliente Node).** Expõe o Str-1 na borda: o `member`
        (opcional) entra no **subscribe** e no **stream_ack** do protocolo binário — depois do `group`, como
        no `fetch` (Str-1): `encode_subscribe_req(topic, group, member, window, max)` e
        `encode_stream_ack_req(topic, group, member, cursor, count)` (`put_str(member)` = flag de presença;
        `nil` = subscription whole-group, sem quebrar o caminho antigo). O `TCPProtocol` despacha por
        presença: `subscribe`/`process_stream_frame` chamam `LogApi.subscribe_member`/`stream_ack_member`
        quando `member != nil and group != nil`, senão o caminho whole-group — **zero range/offset no fio**
        (o push segue records + cursor opaco). Cliente Node: `subscriber.js --member <m>` abre um stream
        escopado, e — fechando o **gap de liveness do membro idle** anotado no Str-1 — um **heartbeat
        periódico** (`setInterval` a 10s < os 30s de session timeout) emite um **ack vazio** (`cursor` nil,
        `count` 0) só quando não houve ack real recente (`lastAck`), mantendo a membership viva; o `SIGINT`
        faz `leaveGroup` (rebalance rápido). `streamAck` ganhou o `member` na assinatura (callers
        whole-group — `loadtest.js` — passam `null`). Testes: round-trip de wire para subscribe/stream_ack
        com/sem member; e2e TCP (`log_streaming_test`) — subscribe como membro único recebe o backlog
        inteiro **opaco** (offset nil), um member ack (commit + heartbeat + credit) é aceito e um produce
        posterior ainda faz push. Suíte 767 testes 0 falhas; credo/dialyzer/format limpos. **G1 (consumer
        groups) + streaming member-scoping concluídos.**
    - 🚧 **Coordinator cluster wiring (épico — consumer groups corretos multi-nó).** Gap que o G1 deixou
      explícito: o `GroupCoordinator` é um GenServer **local por nó** (`Malachi.LogGroupCoordinator`), com
      membership em memória. Num cluster, membros conectados a nós diferentes veem assignments **divergentes**
      → a invariante "cada range sob exatamente um membro" quebra entre nós. Alvo: rotear a coordenação de um
      topic a **um** nó dono, como o NorthGuard roteia requests (broker consulta sua visão local da metadata
      shardada — o `HashRing` sobre vnodes — e encaminha ao vnode dono). Fatiamento: **A1** roteamento +
      encaminhamento · **A2** coordinator no líder do vnode · **A3** teste multi-nó.
      - ✅ **A1 — roteamento do coordinator ao nó dono do vnode + encaminhamento.** Novo módulo **puro**
        `Malachi.Consumer.CoordinatorRouter`: `location(topic, topology, this_node, leader_fn)` roteia
        `topic → vnode` (via `HashRing`), resolve o **líder** do vnode e decide `:local | {:remote, node}`;
        `ref/2` vira o ref de `GenServer` (`{name, node}` se remoto). Roteia por **topic** (co-loca a
        coordenação com o vnode/metadata do topic, onde o `ranges_fun`/`active_range_ids` do coordinator
        resolve contra o broker local). **Fail-safe para `:local`** em toda lacuna de resolução: sem topologia
        (single-node/in-memory), ring vazio, vnode ausente do mapa, ou líder não-resolvível — verificado que
        `:ra.members` num server inexistente **retorna `{:error, :noproc}`** (não levanta), então um vnode
        momentaneamente indisponível degrada a local em vez de derrubar o request. Topologia estática
        (ring + vnode→server_id) publicada **1× no boot** do control plane shardado (`with_metadata_authority`)
        via `:persistent_term` (read lock-free; ausente = single-node → `nil` → local). O `tcp_protocol`
        resolve o ref do coordinator **por request** (`coordinator_for/1`) nos 4 sites (subscribe/fetch/
        stream_ack/leave) e o passa ao `LogApi`; o ref resolvido também vira o `sub.coordinator` (o `leave`
        async do `:DOWN` encaminha ao dono). **Single-node inalterado** (resolve → `:persistent_term` miss →
        nome local). Testado (puro, 9): topologia nil/ring vazio/vnode ausente/líder nil → local; este-nó →
        local; outro-nó → `{:remote}`; `ref/2`; round-trip do `put_topology`/`topology`. Suíte 776 testes 0
        falhas; credo/dialyzer/format limpos. **Limitação conhecida (resolvida no A2):** o `sub.coordinator` era
        o ref resolvido **no subscribe** e não era atualizado nos acks → numa troca de liderança o `leave` do
        `:DOWN` iria ao líder antigo. **Próximo: A2 (consistência de failover) → A3 (teste `:multinode`).**
      - ✅ **A2 (parte A) — consistência de failover: refresh do coordinator no ack + guard de ownership.**
        Fecha a limitação do A1 e endurece a janela de failover, com **membership soft** e **coordinator = líder
        do vnode** — ambos **confirmados pela transcrição do NorthGuard no repo** (`northguard_meetup_transcript.txt`:
        *"this coordinator is the leader of a given VNode... manages all the metadata owned by VNode"*; o Conductor
        do Xinfra faz client-management por conexão/heartbeat, só offsets/checkpoints são duráveis). Decisão
        **A2-A** (foco em correção; o lifecycle "rodar só no líder via `VnodeCoordinatorManager"` entra no A3, junto
        do teste multinode que o exercita — comportamento observável é idêntico, então B é fidelidade de detalhe
        interno só testável multi-nó). **Parte 1 — refresh:** `BrokerServer.stream_ack/7` ganha o param
        `coordinator`; o `handle_call` atualiza `sub.coordinator` (além de `sub.ranges`), então após uma troca de
        líder o `stream_ack_member` (que já re-resolve o líder fresco no `tcp_protocol`) grava o ref novo e o
        `leave` async do `:DOWN` acerta o **dono atual**. **Parte 2 — guard:** o `GroupCoordinator` ganha o seam
        `owns_fun` (default `fn _ -> true end`; no boot, `CoordinatorRouter.owns?/1`); `join`/`poll` rejeitam com
        `{:error, :not_owner}` **sem** registrar quando o nó não lidera o topic (defende contra roteamento stale na
        janela de failover — sem assignment fantasma). O `LogApi` (subscribe/fetch/stream_ack member) propaga o
        `:not_owner` e o `tcp_protocol.subscribe` responde erro em vez de entrar em stream (cliente re-resolve e
        re-subscreve); heartbeat/fetch **auto-curam** no próximo request (roteamento é por-request). Single-node:
        `owns?` é sempre `:local` → nunca rejeita → inalterado. Testado: guard (poll/join → `:not_owner`; sem
        registro fantasma; owns_fun por-topic — 3) + refresh (ack via coordinator diferente → `:DOWN` leave acerta
        o novo — 1). Suíte 780 testes 0 falhas; credo/dialyzer/format limpos. **Próximo: A3 (validação `:multinode`).**
      - ✅ **A3 — validação `:multinode` do roteamento (A1+A2 contra `ra` real).** Prova a máquina do A1/A2 entre nós
        BEAM reais (harness `:peer` + `:erpc`, como o `rebalance_multinode_test`): sobe 3 peers, forma o cluster
        `ra` de um vnode (quorum 2, tolera 1 falha), publica a topologia (`put_topology`) em cada nó, e verifica
        contra a **liderança `ra` viva**: (1) `owns?`/`resolve` concordam — no líder `owns? == true` e `resolve`
        devolve o nome local, em cada follower `owns? == false` e `resolve` devolve `{name, líder}`; (2)
        **forwarding cross-node**: um coordinator no líder recebe dois membros (poll a partir do nó primário via
        `{name, líder}`) e a assignment é **disjunta e completa**; (3) **guard**: um follower rejeita `poll` com
        `{:error, :not_owner}`; (4) **failover**: mata o server `ra` do líder (`:ra.stop_server`) → os 2 restantes
        elegem um novo líder → `owns?`/`resolve` **reconvergem** nele. Detalhes que aterraram o teste (registrados
        p/ o A4): o `server_id` da topologia aponta um **probe** (follower que nunca morre) para o `:ra.members`
        resolver o líder mesmo após o failover; o coordinator no peer sobe via `GenServer.start` **unlinked** (o
        worker do `:erpc` morre e levaria junto um filho linkado); o `ranges_fun` é uma **captura de módulo em
        `test/support`** (`&Fixtures.ranges/1`) — fun anônima do `_test.exs` não é resolvível no peer (não está no
        code path / MD5). `@moduletag :multinode` (excluído por default; roda com `--include multinode`). Suíte 780
        testes 0 falhas (+1 multinode excluído); credo/dialyzer/format limpos. **G1/coordinator: A1+A2+A3 fecham a
        correção multi-nó dos consumer groups. Próximo: A4 (lifecycle — coordinator por-vnode no líder via
        `VnodeCoordinatorManager`, re-validado por este teste; + retry do cliente Node no `:not_owner`).**
      - ✅ **A4 — coordinator por-vnode no líder (lifecycle, o modelo NorthGuard "coordinator = líder do vnode").**
        Antes (A1–A3) **todo nó** rodava um `GroupCoordinator` único e o roteamento mandava o cliente ao dono;
        agora, no control plane **shardado**, cada vnode roda o **seu** coordinator **no líder**, gerido pelo
        `VnodeCoordinatorManager` que já sobe heal/retention por-vnode — start/stop no gate de liderança
        (`MetadataServer.leader?`). Ganhos sobre A1–A3: fidelidade, **isolamento de crash** (um vnode não derruba
        os outros) e **handoff de failover mais limpo** (o manager para no líder velho e sobe fresco no novo).
        **Naming**: o coordinator por-vnode registra sob `CoordinatorRouter.coordinator_name(base, vnode_id)`
        (`Module.concat`, nome local por-vnode — não `:global`, consistente com o roteamento explícito de
        metadata do sistema); o `resolve/2` passa a **derivar** esse nome do vnode roteado (single-node sem
        topologia → nome base). Refactor: `route/2` extraído (DRY entre `location`/`resolve`); o
        `Malachi.LogGroupCoordinator` único vira **condicional** (só non-sharded, em `coordinator_children`);
        `group_coordinator_vnode_child/1` entra no `start_vnode_coordinators/1` (id no supervisor por-vnode,
        `owns_fun` como defesa na janela de flap). O `tcp_protocol` é inalterado (segue passando o nome base ao
        `resolve`). **Single-node inalterado** (boot smoke: coordinator base registrado, `resolve` sem topologia
        → base). O teste `:multinode` do A3 foi **re-validado** com os nomes por-vnode (`coordinator_name`).
        Testado: `coordinator_name/2` puro; multinode 2x verde; boot single-node. Suíte 781 testes 0 falhas
        (+1 `coordinator_name`); credo/dialyzer/format limpos. **Próximo: A5 (retry do cliente Node no
        `:not_owner` — resiliência de cliente na janela de failover).**
      - ✅ **A5 — resiliência do cliente Node no `:not_owner` (janela de failover).** Fecha o item de cliente do
        A4: quando um request de membro é encaminhado a um coordinator que acabou de perder a liderança do vnode,
        o servidor responde `:not_owner` (guard do A2); é **transitório** (o servidor re-resolve o líder atual no
        próximo request), então o cliente deve **retry** em vez de falhar. Node-only (nenhum Elixir): `client.js`
        exporta `isNotOwner(err)` (um `MalachiError` com mensagem `"not_owner"`); `cli.js` ganha `sleep/1`.
        `consumer.js` (fetch por membro) faz **try/catch** no fetch — em `:not_owner`, back-off de 200ms e
        `continue` (retenta; imprime `~`). `subscriber.js` (subscribe por membro) reestrutura o subscribe num
        `startStream()` e, no `onError`, se `:not_owner`, **re-subscreve** após 200ms (em vez de sair) contra o
        novo dono. O `stream_ack` (heartbeat) já era fire-and-forget e auto-cura no próximo ack — sem mudança.
        Validado: `node --check` nos scripts + sanity de `isNotOwner` (não-owner, outra razão, erro não-Malachi).
        Sem harness de teste JS (padrão das fatias de cliente Node anteriores). **A1–A5 fecham o épico de
        coordinator cluster wiring: consumer groups corretos e resilientes multi-nó, fiéis ao NorthGuard.**

### 8.4 Status de adoção e desvios deliberados (retrospectiva)

As ideias de riak_core e k8s acima **já foram absorvidas** pelas fatias da Fase 1/3. O que foi adotado, e
o que foi deliberadamente **não** adotado (com o porquê):

**Adotado (com a fatia que o realizou):**
- **Fencing via consenso** — bootstrap auto-fencido pelo **nome do cluster `ra`** + **Lease sobre `ra`**
  para o trabalho contínuo do líder (R0): triângulo `duração > renew_deadline > retry_period`, token de
  fencing versionado (CAS), largar **proativo** (o *OnStoppedLeading* do k8s) e **relógio do líder** (não
  do cliente — carimbado uma vez e replicado no log, sem clock skew). [k8s Lease + RabbitMQ/`ra`]
- **Staged → planned → committed** — R1 (`desired_placement`) → R2 (`rebalance_plan`) → R3 (`apply_plan`
  sob o lease). [riak_core claimant]
- **Eleição pelo menor nó vivo + fencing** — `membership_leader` / `LeaseHolder`. [ambos]
- **Reconcile level-triggered / idempotente** — os coordinators (heal/retention/rebalance/lease) reconciliam
  **desejado × atual**, idempotentes, só-o-líder-age. [k8s controller]
- **Placement determinístico, rack/DC-aware** — A1 (`spread`) + A2 (`maxSkew` via `place_balanced`) +
  `min_domains`/hard-soft (fatia de hardening de placement), **sem randomização** (raft-safe: toda réplica
  computa o mesmo). [k8s PodTopologySpread `topologyKey`/`maxSkew`/`minDomains`/`whenUnsatisfiable` +
  riak_core binring `target_n_val`]
- **Movimento mínimo** — o HRW é *min-reshuffle* (remover um broker só move o que ele detinha; survivors
  mantêm rank); R1/R2 só movem vnodes afetados; o `heal` preserva réplicas vivas. [riak_core]
- **Add-before-remove** no rebalancing (o quórum nunca cai abaixo no meio da mudança). [riak_core/`ra`]

**Desvios deliberados / não adotado:**
- **workqueue + expectations** (k8s controller): **não** adotado. Os coordinators são **síncronos, por
  tick** (level-triggered simples), sem fila com dedupe/rate-limit nem rastreamento de operações em voo com
  TTL. Justificativa: a cardinalidade do reconcile é baixa (poucos vnodes por tick), o `ra` já **serializa**
  as mudanças de membership (uma por vez, com retry em `:cluster_change_not_permitted`) e o commit de
  rebalancing é **manual** — não há a explosão de eventos que motiva workqueue/expectations no k8s.
  Reavaliar **se** um gatilho automático de rebalancing for adicionado.
- **sticky preference no `heal`** (k8s): não precisou de código dedicado — o HRW já prefere réplicas
  sobreviventes **inerentemente** (min-reshuffle). Mesma propriedade, de graça.
- **`target_n_val` "nós E locations distintos"** (riak_core): coberto por **composição**, não por um
  parâmetro único — `Placement.place` já devolve brokers **distintos** (nós distintos) e `min_domains`
  garante **domínios distintos**; juntos ≡ `target_n_val`.
- **binring V4 (min-movement exato)** (riak_core `update()` antes de `solve()`): usamos HRW (movimento
  mínimo **estatístico**) + `maxSkew` (A2) para uniformidade — suficiente para o control plane (poucos
  vnodes); não portamos o algoritmo exato do binring.
- **etcd único** (k8s): não adotado — é justamente o **gargalo de escala** (~5000 nodes) que o sharding por
  vnode (fatia D) evita.

**Ainda aberto (por cima do mesmo motor):**
- ✅ **Gatilho automático de rebalancing (feito, opt-in).** `Malachi.Cluster.AutoRebalancer` — **política
  level-triggered** por cima do mecanismo `RebalanceCoordinator` (que segue manual-by-default). A cada tick
  (default 30s), **só no holder do lease**: pega o `plan`; se **não-vazio e igual** por `stabilization`
  ticks consecutivos (default 3 ≈ 90s), chama `commit` (que re-gate o líder). Plan vazio ou que mudou →
  reseta o contador — assim um **flap do SWIM** (nó brevemente suspeito → volta) **nunca** move vnode. É o
  padrão que a tabela acima registrou: SWIM faz a *detecção* event-driven; a *decisão* de mover reconcilia
  e converge (nada de eventos perdidos). Seams (`plan_fun`/`commit_fun`/`leader?`) → testável sem `ra`/lease
  dirigindo `reconcile_now`. **Opt-in** (`MALACHIMQ_AUTO_REBALANCE`, default off = comportamento manual
  atual intacto); `interval`/`stabilization` configuráveis; subido no `rebalance_children` só quando
  sharded + habilitado. Testado: commit após N ticks estáveis; plan que muda reseta a janela; plan vazio
  nunca commita; não-líder não commita; `stabilization: 1` commita na 1ª observação; perda/reganho de
  liderança reseta; resultado de falha-parcial repassado ao `on_result` — 7. Suíte 738 testes 0 falhas;
  credo/dialyzer limpos. README com os env vars. *(workqueue/expectations do k8s seguem **não** adotados —
  a cardinalidade é baixa, o `ra` serializa a membership e a estabilização já dá o debounce.)*
- **Re-sharding** — mudar a **contagem** de vnodes (R1/R2 assumem o mesmo conjunto de vnode ids).
